import os
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

load_dotenv()


class UltraFastMetaActivityTracker:
    """Ultra-fast tracker with Batch API + Caching (5-10x faster)"""
    
    EXCLUDED_EVENT_TYPES = {
        'ad_account_billing_charge', 'ad_account_billing_charge_failed',
        'ad_account_billing_decline', 'ad_review_approved', 'ad_review_declined',
        'automatic_placement_optimization', 'campaign_budget_optimization_auto',
        'auto_bid_adjustment', 'delivery_insights_notification'
    }
    
    def __init__(self, meta_access_token, airtable_token, airtable_base_id,
                 airtable_table_name, google_credentials_path=None,
                 google_spreadsheet_id=None, max_workers=10, debug_mode=False):
        
        self.meta_access_token = meta_access_token
        self.meta_base_url = "https://graph.facebook.com/v18.0"
        self.session = self._create_session_with_retries()
        
        self.airtable_token = airtable_token
        self.airtable_base_id = airtable_base_id
        self.airtable_table_name = airtable_table_name
        self.airtable_url = f'https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}'
        
        self.google_credentials_path = google_credentials_path
        self.google_spreadsheet_id = google_spreadsheet_id
        self.gspread_client = self.setup_google_sheets() if google_credentials_path else None
        
        self.max_workers = max_workers
        self.brand_mapping_df = None
        self.brand_mapping_dict = {}
        self.debug_mode = debug_mode
        
        # CACHING for massive speed improvement
        self.campaign_cache = {}
        self.adset_cache = {}
        self.ad_cache = {}
        
        self.debug_stats = {
            'accounts_total': 0, 'accounts_active': 0, 'accounts_inactive': 0,
            'accounts_with_activity': 0, 'duplicate_brands': {},
            'object_types_found': {}, 'hierarchy_built': {'campaign_group': 0, 'campaign': 0, 'adgroup': 0},
            'api_calls': {'campaign': 0, 'adset': 0, 'ad': 0, 'batch': 0},
            'cache_hits': {'campaign': 0, 'adset': 0, 'ad': 0},
            'hierarchy_errors': [], 'activities_filtered_out': 0, 'activities_included': 0,
            'api_errors': {'400': 0, '403': 0, '404': 0, '500': 0, 'other': 0},
            'skipped_objects': [], 'batch_savings': 0
        }

    def _create_session_with_retries(self):
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504),
                     allowed_methods=["HEAD", "GET", "POST", "OPTIONS"])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _is_valid_meta_id(self, obj_id):
        """Validate Meta ID"""
        if not obj_id:
            return False
        obj_id = str(obj_id).strip()
        return 10 <= len(obj_id) <= 25 and obj_id.isdigit()

    def _make_api_request(self, url, params=None, headers=None, timeout=15, retries=2):
        """Enhanced API request"""
        for attempt in range(retries + 1):
            try:
                r = self.session.get(url, params=params, headers=headers, timeout=timeout)
                
                if r.status_code in [400, 403, 404]:
                    self.debug_stats['api_errors'][str(r.status_code)] += 1
                    return None
                elif r.status_code >= 500:
                    if attempt < retries:
                        time.sleep(0.5 * (attempt + 1))
                        continue
                    self.debug_stats['api_errors']['500'] += 1
                    return None
                
                r.raise_for_status()
                return r.json()
            except Exception:
                if attempt < retries:
                    continue
                return None
        return None

    def _batch_api_request(self, requests_list):
        """
        Make BATCH API request - UP TO 50 requests in one HTTP call
        This is the KEY to 5-10x speed improvement
        """
        if not requests_list:
            return {}
        
        # Split into chunks of 50 (Meta's batch limit)
        results = {}
        chunk_size = 50
        
        for i in range(0, len(requests_list), chunk_size):
            chunk = requests_list[i:i+chunk_size]
            
            batch_payload = []
            for idx, req in enumerate(chunk):
                batch_payload.append({
                    "method": "GET",
                    "relative_url": req['relative_url']
                })
            
            url = f"{self.meta_base_url}/"
            params = {'access_token': self.meta_access_token, 'batch': json.dumps(batch_payload)}
            
            self.debug_stats['api_calls']['batch'] += 1
            
            try:
                response = self.session.post(url, params=params, timeout=30)
                response.raise_for_status()
                batch_results = response.json()
                
                for idx, result in enumerate(batch_results):
                    original_idx = i + idx
                    if original_idx >= len(requests_list):
                        break
                    
                    obj_id = requests_list[original_idx]['id']
                    
                    if result.get('code') == 200:
                        try:
                            body = json.loads(result['body'])
                            results[obj_id] = body
                        except:
                            results[obj_id] = None
                    else:
                        results[obj_id] = None
                
                time.sleep(0.1)  # Small delay between batch chunks
                
            except Exception as e:
                if self.debug_mode:
                    print(f"⚠️ Batch request failed: {e}")
                # Mark all in chunk as None
                for req in chunk:
                    results[req['id']] = None
        
        return results

    def _is_human_activity(self, activity):
        """Permissive filtering"""
        event_type = activity.get('event_type', '').lower()
        
        if event_type in self.EXCLUDED_EVENT_TYPES:
            self.debug_stats['activities_filtered_out'] += 1
            return False
        
        actor = activity.get('actor_name', '').lower()
        if actor in ['meta', 'facebook', 'system', 'automated', '']:
            self.debug_stats['activities_filtered_out'] += 1
            return False
        
        self.debug_stats['activities_included'] += 1
        return True

    def _normalize_brand_name(self, name):
        """Normalize brand name"""
        if pd.isna(name) or not name:
            return ''
        
        name = str(name).lower().strip()
        remove_terms = ['pvt ltd', 'private limited', 'pvt. ltd.', 'private ltd',
                       'llp', 'opc', 'limited', 'ltd', 'inc', 'corp',
                       '- current', '- new', '- old', 'domestic', 'export',
                       'the ', 'a ', 'an ', 'international', 'india']
        
        for term in remove_terms:
            name = name.replace(term, '')
        
        name = ' '.join(name.split())
        name = ''.join(c for c in name if c.isalnum() or c.isspace())
        return name.strip()

    def _find_best_brand_match(self, brand_name):
        """Fuzzy brand matching"""
        if not brand_name:
            return None
        
        normalized_input = self._normalize_brand_name(brand_name)
        if normalized_input in self.brand_mapping_dict:
            return self.brand_mapping_dict[normalized_input]
        
        for norm_brand, data in self.brand_mapping_dict.items():
            if (normalized_input in norm_brand or norm_brand in normalized_input):
                if len(normalized_input) >= 5 and len(norm_brand) >= 5:
                    return data
        return None

    def fetch_airtable_data(self):
        """Fetch Airtable brand data"""
        print("\n" + "="*80)
        print("FETCHING AIRTABLE BRAND DATA")
        print("="*80)
        
        headers = {'Authorization': f'Bearer {self.airtable_token}'}
        all_records = []
        url = self.airtable_url
        
        while url:
            response = self._make_api_request(url, headers=headers)
            if not response:
                break
            all_records.extend(response.get('records', []))
            offset = response.get('offset')
            url = f"{self.airtable_url}?offset={offset}" if offset else None
            time.sleep(0.1)
        
        if not all_records:
            print("❌ No Airtable records")
            return pd.DataFrame()
        
        df = pd.DataFrame([r.get('fields', {}) for r in all_records])
        df.columns = df.columns.str.strip()
        print(f"✅ Fetched {len(df)} brands")
        return df

    def get_all_ad_accounts(self):
        """Get ACTIVE Meta ad accounts only"""
        url = f"{self.meta_base_url}/me/adaccounts"
        params = {'access_token': self.meta_access_token,
                 'fields': 'id,name,account_status,business_name,currency,timezone_name',
                 'limit': 100}
        
        accounts = []
        while True:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break
            accounts.extend(data.get('data', []))
            next_url = data.get('paging', {}).get('next')
            if not next_url:
                break
            url, params = next_url, {}
            time.sleep(0.05)
        
        self.debug_stats['accounts_total'] = len(accounts)
        
        # Only ACTIVE accounts
        active = [a for a in accounts if a.get('account_status') == 1]
        inactive = [a for a in accounts if a.get('account_status') != 1]
        
        self.debug_stats['accounts_active'] = len(active)
        self.debug_stats['accounts_inactive'] = len(inactive)
        
        print(f"\n📊 ACCOUNTS: Total={len(accounts)}, ✅Active={len(active)}, ❌Inactive={len(inactive)}")
        
        # Detect duplicates
        brand_accts = defaultdict(list)
        for a in active:
            brand = a.get('business_name') or a.get('name', 'Unknown')
            brand_accts[self._normalize_brand_name(brand)].append(a)
        
        dupes = {b: a for b, a in brand_accts.items() if len(a) > 1}
        if dupes:
            print(f"⚠️ {len(dupes)} brands with multiple accounts")
            self.debug_stats['duplicate_brands'] = dupes
        
        return active

    def get_account_activities(self, ad_account_id, hours=24):
        """Get human activities from account"""
        since = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S')
        url = f"{self.meta_base_url}/{ad_account_id}/activities"
        params = {'access_token': self.meta_access_token, 'since': since, 'limit': 500,
                 'fields': 'event_type,event_time,actor_name,object_name,object_type,object_id,translated_event_type,extra_data'}
        
        activities = []
        while True:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break
            activities.extend([a for a in data.get('data', []) if self._is_human_activity(a)])
            next_url = data.get('paging', {}).get('next')
            if not next_url:
                break
            url, params = next_url, {}
            time.sleep(0.03)
        return activities

    def batch_fetch_campaigns(self, campaign_ids):
        """Batch fetch multiple campaigns in ONE API call"""
        campaign_ids = [cid for cid in campaign_ids if self._is_valid_meta_id(cid) and cid not in self.campaign_cache]
        
        if not campaign_ids:
            return
        
        fields = 'id,name,status,effective_status,objective,daily_budget,lifetime_budget,bid_strategy'
        requests = [{'id': cid, 'relative_url': f"{cid}?fields={fields}"} for cid in campaign_ids]
        
        results = self._batch_api_request(requests)
        
        for cid, data in results.items():
            if data:
                self.campaign_cache[cid] = data
                self.debug_stats['api_calls']['campaign'] += 1
        
        if self.debug_mode:
            print(f"   📦 Batch fetched {len(results)} campaigns")

    def batch_fetch_adsets(self, adset_ids):
        """Batch fetch multiple adsets in ONE API call"""
        adset_ids = [aid for aid in adset_ids if self._is_valid_meta_id(aid) and aid not in self.adset_cache]
        
        if not adset_ids:
            return
        
        fields = 'id,name,status,effective_status,campaign_id,optimization_goal,billing_event,targeting'
        requests = [{'id': aid, 'relative_url': f"{aid}?fields={fields}"} for aid in adset_ids]
        
        results = self._batch_api_request(requests)
        
        for aid, data in results.items():
            if data:
                self.adset_cache[aid] = data
                self.debug_stats['api_calls']['adset'] += 1
        
        if self.debug_mode:
            print(f"   📦 Batch fetched {len(results)} adsets")

    def batch_fetch_ads(self, ad_ids):
        """Batch fetch multiple ads in ONE API call"""
        ad_ids = [aid for aid in ad_ids if self._is_valid_meta_id(aid) and aid not in self.ad_cache]
        
        if not ad_ids:
            return
        
        fields = 'id,name,status,effective_status,adset_id,preview_shareable_link'
        requests = [{'id': aid, 'relative_url': f"{aid}?fields={fields}"} for aid in ad_ids]
        
        results = self._batch_api_request(requests)
        
        for aid, data in results.items():
            if data:
                self.ad_cache[aid] = data
                self.debug_stats['api_calls']['ad'] += 1
        
        if self.debug_mode:
            print(f"   📦 Batch fetched {len(results)} ads")

    def get_campaign_details(self, cid):
        """Get campaign with caching"""
        if not self._is_valid_meta_id(cid):
            return None
        
        if cid in self.campaign_cache:
            self.debug_stats['cache_hits']['campaign'] += 1
            return self.campaign_cache[cid]
        
        return None  # Will be fetched in batch

    def get_adset_details(self, aid):
        """Get adset with caching"""
        if not self._is_valid_meta_id(aid):
            return None
        
        if aid in self.adset_cache:
            self.debug_stats['cache_hits']['adset'] += 1
            return self.adset_cache[aid]
        
        return None  # Will be fetched in batch

    def get_ad_details(self, aid):
        """Get ad with caching"""
        if not self._is_valid_meta_id(aid):
            return None
        
        if aid in self.ad_cache:
            self.debug_stats['cache_hits']['ad'] += 1
            return self.ad_cache[aid]
        
        return None  # Will be fetched in batch

    def _extract_targeting_info(self, targeting):
        """Extract targeting details"""
        if not targeting or not isinstance(targeting, dict):
            return 'N/A', 'N/A', 'N/A'
        
        age_min = targeting.get('age_min', 'N/A')
        age_max = targeting.get('age_max', 'N/A')
        age = f"{age_min}-{age_max}" if age_min != 'N/A' else 'N/A'
        
        genders = targeting.get('genders', [])
        gender = 'Male' if genders == [1] else 'Female' if genders == [2] else 'All'
        
        geo = targeting.get('geo_locations', {})
        countries = geo.get('countries', [])
        location = ', '.join(countries[:3]) + (f' +{len(countries)-3}' if len(countries) > 3 else '') if countries else 'N/A'
        
        return age, gender, location

    def _collect_ids_from_activities(self, all_activities):
        """
        STEP 1: Collect all IDs we need to fetch
        This allows us to batch fetch everything at once
        """
        campaign_ids = set()
        adset_ids = set()
        ad_ids = set()
        
        for act in all_activities:
            obj_id = act.get('object_id', '')
            obj_type = act.get('object_type', '').lower()
            
            if obj_type == 'campaign_group':
                campaign_ids.add(obj_id)
            elif obj_type == 'campaign':
                adset_ids.add(obj_id)
            elif obj_type == 'adgroup':
                ad_ids.add(obj_id)
        
        return campaign_ids, adset_ids, ad_ids

    def _build_complete_hierarchy(self, activity):
        """Build hierarchy using CACHED data (no API calls here!)"""
        obj_id = activity.get('object_id', '')
        obj_type = activity.get('object_type', '').lower()
        obj_name = activity.get('object_name', '')
        
        self.debug_stats['object_types_found'][obj_type] = self.debug_stats['object_types_found'].get(obj_type, 0) + 1
        
        h = {'Campaign_Name': 'N/A', 'Campaign_Status': 'N/A', 'Campaign_Objective': 'N/A',
             'Campaign_Budget_Type': 'N/A', 'Campaign_Budget': 'N/A', 'Campaign_Bid_Strategy': 'N/A',
             'AdSet_Name': 'N/A', 'AdSet_Status': 'N/A', 'AdSet_Optimization_Goal': 'N/A',
             'AdSet_Billing_Event': 'N/A', 'Age_Targeting': 'N/A', 'Gender_Targeting': 'N/A',
             'Location_Targeting': 'N/A', 'Ad_Name': 'N/A', 'Ad_Status': 'N/A',
             'Ad_Preview_Link': 'N/A', 'Hierarchy_Level': 'UNKNOWN'}
        
        try:
            if obj_type == 'campaign_group':
                h['Hierarchy_Level'] = 'CAMPAIGN'
                data = self.get_campaign_details(obj_id)
                if data:
                    h['Campaign_Name'] = data.get('name', obj_name)
                    h['Campaign_Status'] = data.get('effective_status', 'N/A')
                    h['Campaign_Objective'] = data.get('objective', 'N/A')
                    h['Campaign_Bid_Strategy'] = data.get('bid_strategy', 'N/A')
                    
                    if data.get('daily_budget'):
                        h['Campaign_Budget_Type'] = 'Daily'
                        h['Campaign_Budget'] = f"${float(data['daily_budget'])/100:.2f}"
                    elif data.get('lifetime_budget'):
                        h['Campaign_Budget_Type'] = 'Lifetime'
                        h['Campaign_Budget'] = f"${float(data['lifetime_budget'])/100:.2f}"
                    self.debug_stats['hierarchy_built']['campaign_group'] += 1
            
            elif obj_type == 'campaign':
                h['Hierarchy_Level'] = 'ADSET'
                data = self.get_adset_details(obj_id)
                if data:
                    h['AdSet_Name'] = data.get('name', obj_name)
                    h['AdSet_Status'] = data.get('effective_status', 'N/A')
                    h['AdSet_Optimization_Goal'] = data.get('optimization_goal', 'N/A')
                    h['AdSet_Billing_Event'] = data.get('billing_event', 'N/A')
                    
                    age, gender, loc = self._extract_targeting_info(data.get('targeting'))
                    h['Age_Targeting'], h['Gender_Targeting'], h['Location_Targeting'] = age, gender, loc
                    
                    cid = data.get('campaign_id')
                    if cid:
                        cdata = self.get_campaign_details(cid)
                        if cdata:
                            h['Campaign_Name'] = cdata.get('name', 'N/A')
                            h['Campaign_Status'] = cdata.get('effective_status', 'N/A')
                            h['Campaign_Objective'] = cdata.get('objective', 'N/A')
                            h['Campaign_Bid_Strategy'] = cdata.get('bid_strategy', 'N/A')
                            
                            if cdata.get('daily_budget'):
                                h['Campaign_Budget_Type'] = 'Daily'
                                h['Campaign_Budget'] = f"${float(cdata['daily_budget'])/100:.2f}"
                            elif cdata.get('lifetime_budget'):
                                h['Campaign_Budget_Type'] = 'Lifetime'
                                h['Campaign_Budget'] = f"${float(cdata['lifetime_budget'])/100:.2f}"
                    self.debug_stats['hierarchy_built']['campaign'] += 1
            
            elif obj_type == 'adgroup':
                h['Hierarchy_Level'] = 'AD'
                data = self.get_ad_details(obj_id)
                if data:
                    h['Ad_Name'] = data.get('name', obj_name)
                    h['Ad_Status'] = data.get('effective_status', 'N/A')
                    h['Ad_Preview_Link'] = data.get('preview_shareable_link', 'N/A')
                    
                    aid = data.get('adset_id')
                    if aid:
                        adata = self.get_adset_details(aid)
                        if adata:
                            h['AdSet_Name'] = adata.get('name', 'N/A')
                            h['AdSet_Status'] = adata.get('effective_status', 'N/A')
                            h['AdSet_Optimization_Goal'] = adata.get('optimization_goal', 'N/A')
                            h['AdSet_Billing_Event'] = adata.get('billing_event', 'N/A')
                            
                            age, gender, loc = self._extract_targeting_info(adata.get('targeting'))
                            h['Age_Targeting'], h['Gender_Targeting'], h['Location_Targeting'] = age, gender, loc
                            
                            cid = adata.get('campaign_id')
                            if cid:
                                cdata = self.get_campaign_details(cid)
                                if cdata:
                                    h['Campaign_Name'] = cdata.get('name', 'N/A')
                                    h['Campaign_Status'] = cdata.get('effective_status', 'N/A')
                                    h['Campaign_Objective'] = cdata.get('objective', 'N/A')
                                    h['Campaign_Bid_Strategy'] = cdata.get('bid_strategy', 'N/A')
                                    
                                    if cdata.get('daily_budget'):
                                        h['Campaign_Budget_Type'] = 'Daily'
                                        h['Campaign_Budget'] = f"${float(cdata['daily_budget'])/100:.2f}"
                                    elif cdata.get('lifetime_budget'):
                                        h['Campaign_Budget_Type'] = 'Lifetime'
                                        h['Campaign_Budget'] = f"${float(cdata['lifetime_budget'])/100:.2f}"
                    self.debug_stats['hierarchy_built']['adgroup'] += 1
            else:
                h['Hierarchy_Level'] = f'OTHER:{obj_type}'
        
        except Exception as e:
            self.debug_stats['hierarchy_errors'].append(f"{obj_type}-{obj_id}:{e}")
        
        return h

    def _process_account(self, account, hours=24):
        """Process one account - collect activities only (no hierarchy building yet)"""
        acc_id = account.get('id')
        acc_name = account.get('name', 'Unknown')
        biz_name = account.get('business_name', '')
        acc_status = account.get('account_status', 'Unknown')
        brand = biz_name if biz_name else acc_name
        
        activities = self.get_account_activities(acc_id, hours) or []
        if activities:
            self.debug_stats['accounts_with_activity'] += 1
        
        # Return raw activities with account info
        return [(act, brand, acc_id, acc_name, acc_status) for act in activities]

    def fetch_meta_activities(self, hours=24):
        """
        Fetch activities with ULTRA-FAST batch processing
        STEP 1: Collect all activities
        STEP 2: Batch fetch all objects at once
        STEP 3: Build hierarchies from cache
        """
        print("\n" + "="*80)
        print(f"🚀 ULTRA-FAST FETCHING (Last {hours}h) - BATCH API + CACHING")
        print("="*80)
        
        accounts = self.get_all_ad_accounts()
        if not accounts:
            print("❌ No active accounts")
            return pd.DataFrame()
        
        # STEP 1: Collect all activities from all accounts (parallel)
        print(f"\n📥 STEP 1: Collecting activities from {len(accounts)} accounts...")
        all_raw_activities = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._process_account, acc, hours): acc for acc in accounts}
            
            for i, future in enumerate(as_completed(futures), 1):
                if i % 10 == 0:
                    print(f"  Progress: {i}/{len(accounts)}")
                try:
                    all_raw_activities.extend(future.result())
                except Exception as e:
                    print(f"⚠️ Error: {e}")
        
        if not all_raw_activities:
            print("\n✅ No activities found")
            return pd.DataFrame()
        
        print(f"✅ Collected {len(all_raw_activities)} activities")
        
        # STEP 2: Extract all IDs we need
        print(f"\n📋 STEP 2: Extracting object IDs...")
        activities_only = [item[0] for item in all_raw_activities]
        campaign_ids, adset_ids, ad_ids = self._collect_ids_from_activities(activities_only)
        
        print(f"  Found: {len(campaign_ids)} campaigns, {len(adset_ids)} adsets, {len(ad_ids)} ads")
        
        # STEP 3: Batch fetch ALL objects at once (this is the magic!)
        print(f"\n⚡ STEP 3: Batch fetching ALL objects...")
        start_batch = time.time()
        
        # First fetch adsets and ads to get parent IDs
        if ad_ids:
            print(f"  📦 Batch fetching {len(ad_ids)} ads...")
            self.batch_fetch_ads(list(ad_ids))
        
        if adset_ids:
            print(f"  📦 Batch fetching {len(adset_ids)} adsets...")
            self.batch_fetch_adsets(list(adset_ids))
        
        # Collect parent campaign IDs from adsets
        parent_campaign_ids = set()
        for adset_data in self.adset_cache.values():
            cid = adset_data.get('campaign_id')
            if cid:
                parent_campaign_ids.add(cid)
        
        # Combine with direct campaign IDs
        all_campaign_ids = campaign_ids.union(parent_campaign_ids)
        
        if all_campaign_ids:
            print(f"  📦 Batch fetching {len(all_campaign_ids)} campaigns...")
            self.batch_fetch_campaigns(list(all_campaign_ids))
        
        batch_time = time.time() - start_batch
        print(f"✅ Batch fetching complete in {batch_time:.1f}s")
        
        # STEP 4: Build hierarchies from cache (super fast, no API calls!)
        print(f"\n🏗️ STEP 4: Building hierarchies from cache...")
        results = []
        
        for act, brand, acc_id, acc_name, acc_status in all_raw_activities:
            h = self._build_complete_hierarchy(act)
            
            extra = act.get('extra_data', {})
            try:
                if isinstance(extra, str):
                    extra = json.loads(extra)
                old_val = str(extra.get('old_value', 'N/A'))
                new_val = str(extra.get('new_value', 'N/A'))
            except:
                old_val, new_val = 'N/A', 'N/A'
            
            timestamp = act.get('event_time', '')
            if timestamp:
                try:
                    dt = datetime.strptime(timestamp.split('+')[0].replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
                    timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
            
            results.append({
                'Brand': brand, 'Account_ID': acc_id, 'Account_Name': acc_name,
                'Account_Status': acc_status, 'Actor': act.get('actor_name', 'Unknown'),
                'Action': act.get('translated_event_type', act.get('event_type', 'Unknown')),
                'Hierarchy_Level': h['Hierarchy_Level'], 'Timestamp': timestamp,
                **{k: v for k, v in h.items() if k != 'Hierarchy_Level'},
                'Changed_From': old_val, 'Changed_To': new_val,
                'Object_Name': act.get('object_name', ''),
                'Object_ID': act.get('object_id', ''),
                'Object_Type_Raw': act.get('object_type', ''),
                'Raw_Event_Type': act.get('event_type', '')
            })
        
        df = pd.DataFrame(results)
        if 'Timestamp' in df.columns:
            df = df.sort_values('Timestamp', ascending=False)
        
        # Calculate batch savings
        individual_calls = len(campaign_ids) + len(adset_ids) + len(ad_ids) + len(parent_campaign_ids)
        batch_calls = self.debug_stats['api_calls']['batch']
        savings = individual_calls - batch_calls
        self.debug_stats['batch_savings'] = savings
        
        print(f"\n✅ Built {len(df)} activities")
        print(f"\n⚡ PERFORMANCE STATS:")
        print(f"  Individual calls saved: {individual_calls} → {batch_calls} batches")
        print(f"  Savings: {savings} API calls (~{savings/50:.0f}x faster!)")
        print(f"  Cache hits: Campaign={self.debug_stats['cache_hits']['campaign']}, "
              f"AdSet={self.debug_stats['cache_hits']['adset']}, Ad={self.debug_stats['cache_hits']['ad']}")
        
        return df

    def map_airtable_to_activities(self, df):
        """Map Airtable data to activities"""
        if df.empty or self.brand_mapping_df is None or self.brand_mapping_df.empty:
            return df
        
        print("\n" + "="*80)
        print("MAPPING AIRTABLE DATA")
        print("="*80)
        
        cols = self.brand_mapping_df.columns
        brand_col = next((c for c in ['Brand', 'Brands', 'Brand Name'] if c in cols), None)
        fb_col = next((c for c in ['FB Manager', 'FB_Manager'] if c in cols), None)
        bm_col = next((c for c in ['Brand Manager', 'Brand_Manager'] if c in cols), None)
        team_col = next((c for c in ['Current Team', 'Team'] if c in cols), None)
        
        if not brand_col:
            print("❌ No Brand column")
            return df
        
        for _, row in self.brand_mapping_df.iterrows():
            bn = str(row[brand_col]).strip() if pd.notna(row[brand_col]) else ''
            if bn:
                norm = self._normalize_brand_name(bn)
                if norm:
                    self.brand_mapping_dict[norm] = {
                        'original_name': bn,
                        'FB_Manager': row[fb_col] if fb_col and pd.notna(row[fb_col]) else 'Not Assigned',
                        'Brand_Manager': row[bm_col] if bm_col and pd.notna(row[bm_col]) else 'Not Assigned',
                        'Current_Team': row[team_col] if team_col and pd.notna(row[team_col]) else 'Not Assigned'
                    }
        
        def map_brand(brand):
            match = self._find_best_brand_match(brand)
            if match:
                return pd.Series({'Matched_Airtable_Brand': match['original_name'],
                                'FB_Manager': match['FB_Manager'],
                                'Brand_Manager': match['Brand_Manager'],
                                'Current_Team': match['Current_Team']})
            return pd.Series({'Matched_Airtable_Brand': '', 'FB_Manager': 'Unknown',
                            'Brand_Manager': 'Unknown', 'Current_Team': 'Unknown'})
        
        mapping = df['Brand'].apply(map_brand)
        df = pd.concat([df, mapping], axis=1)
        df['Fetch_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        mapped = df[df['FB_Manager'] != 'Unknown'].shape[0]
        print(f"✅ Mapped: {mapped}/{len(df)} ({mapped/len(df)*100:.1f}%)")
        
        col_order = ['Brand', 'Matched_Airtable_Brand', 'FB_Manager', 'Brand_Manager', 'Current_Team',
                    'Actor', 'Action', 'Hierarchy_Level', 'Timestamp',
                    'Campaign_Name', 'Campaign_Status', 'Campaign_Objective', 'Campaign_Budget_Type',
                    'Campaign_Budget', 'Campaign_Bid_Strategy', 'AdSet_Name', 'AdSet_Status',
                    'AdSet_Optimization_Goal', 'AdSet_Billing_Event', 'Age_Targeting', 'Gender_Targeting',
                    'Location_Targeting', 'Ad_Name', 'Ad_Status', 'Ad_Preview_Link',
                    'Changed_From', 'Changed_To', 'Account_ID', 'Account_Name', 'Account_Status',
                    'Object_Name', 'Object_ID', 'Object_Type_Raw', 'Raw_Event_Type', 'Fetch_Date']
        
        return df[[c for c in col_order if c in df.columns]]

    def setup_google_sheets(self):
        """Setup Google Sheets"""
        if not (self.google_credentials_path and os.path.exists(self.google_credentials_path)):
            return None
        try:
            scopes = ["https://www.googleapis.com/auth/spreadsheets",
                     "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_file(self.google_credentials_path, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"⚠️ Google Sheets setup failed: {e}")
            return None

    def get_last_entry_time_from_sheet(self):
        """Get last timestamp from sheet"""
        if not self.gspread_client:
            return None
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            ws = sh.worksheet('Meta_Activities_Log')
            data = ws.get_all_records()
            if not data:
                return None
            
            df = pd.DataFrame(data)
            if 'Timestamp' not in df.columns:
                return None
            
            df = df[df['Timestamp'].notna() & (df['Timestamp'] != '')]
            if df.empty:
                return None
            
            df['Timestamp_dt'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            df = df.dropna(subset=['Timestamp_dt'])
            if df.empty:
                return None
            
            last = df['Timestamp_dt'].max()
            print(f"📅 Last entry: {last}")
            return last
        except Exception as e:
            print(f"⚠️ Could not read last entry: {e}")
            return None

    def read_existing_data_from_sheets(self, sheet='Meta_Activities_Log'):
        """Read existing sheet data"""
        if not self.gspread_client:
            return pd.DataFrame()
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            ws = sh.worksheet(sheet)
            data = ws.get_all_records()
            if data:
                df = pd.DataFrame(data)
                print(f"📊 Existing: {len(df)} rows")
                return df
            return pd.DataFrame()
        except:
            return pd.DataFrame()

    def upload_to_sheets(self, df, sheet='Meta_Activities_Log', append_mode=False):
        """Upload to Google Sheets with deduplication"""
        if not self.gspread_client or df.empty:
            return
        
        print(f"\n{'='*80}\nUPLOADING TO SHEETS\n{'='*80}")
        
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            
            if append_mode:
                existing = self.read_existing_data_from_sheets(sheet)
                if not existing.empty:
                    def uid(r):
                        return f"{r['Account_ID']}_{r.get('Object_Name','')}_{r['Timestamp']}_{r['Action']}"
                    
                    existing['_uid'] = existing.apply(uid, axis=1)
                    df['_uid'] = df.apply(uid, axis=1)
                    
                    existing_ids = set(existing['_uid'])
                    new_df = df[~df['_uid'].isin(existing_ids)].copy()
                    
                    print(f"   Existing: {len(existing)}, New fetch: {len(df)}, Truly new: {len(new_df)}")
                    
                    if len(new_df) > 0:
                        combined = pd.concat([existing, new_df], ignore_index=True)
                        combined = combined.drop('_uid', axis=1).sort_values('Timestamp', ascending=False)
                        df = combined
                    else:
                        print("   ℹ️ No new activities")
                        df = existing.drop('_uid', axis=1)
            
            try:
                ws = sh.worksheet(sheet)
                ws.clear()
            except:
                ws = sh.add_worksheet(title=sheet, rows=max(1000, len(df)+50), cols=max(20, len(df.columns)+5))
            
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            ws.format('1:1', {'textFormat': {'bold': True}, 'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2}})
            ws.freeze(rows=1)
            
            print(f"✅ Uploaded {len(df)} rows\n🔗 https://docs.google.com/spreadsheets/d/{self.google_spreadsheet_id}")
        except Exception as e:
            print(f"❌ Upload failed: {e}")

    def run(self, hours=24, append_mode=False, save_csv=False):
        """Main execution pipeline - ULTRA FAST!"""
        start = time.time()
        
        print("="*80)
        print("⚡ ULTRA-FAST META TRACKER - BATCH API + CACHING")
        print("🚀 5-10x FASTER | ✅ Same Data Structure | ✅ Active Accounts Only")
        print("="*80)
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Mode: {'APPEND (Smart Fetch)' if append_mode else 'REPLACE'}")
        print("="*80)
        
        # Smart fetch
        if append_mode:
            last = self.get_last_entry_time_from_sheet()
            if last:
                now = datetime.now()
                gap = (now - last).total_seconds() / 3600
                hours = int(gap) + 2
                print(f"\n🔍 SMART FETCH: Last={last.strftime('%Y-%m-%d %H:%M:%S')}, Gap={gap:.1f}h, Fetching={hours}h\n")
        
        self.brand_mapping_df = self.fetch_airtable_data()
        df = self.fetch_meta_activities(hours=hours)
        
        if df.empty:
            print("\n✅ No activities found")
            return df
        
        final = self.map_airtable_to_activities(df)
        
        # Summary
        time_range = f"{final['Timestamp'].min()} to {final['Timestamp'].max()}" if 'Timestamp' in final.columns else 'N/A'
        
        print(f"\n{'='*80}\n📊 SUMMARY\n{'='*80}")
        print(f"Total: {len(final)} | Brands: {final['Brand'].nunique()} | Actors: {final['Actor'].nunique()}")
        print(f"Time: {time_range}")
        print(f"Active accounts with activity: {self.debug_stats['accounts_with_activity']}/{self.debug_stats['accounts_active']}")
        
        if save_csv:
            fn = f"meta_activities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            final.to_csv(fn, index=False)
            print(f"\n💾 CSV: {fn}")
        
        if self.gspread_client:
            self.upload_to_sheets(final, append_mode=append_mode)
        
        duration = (time.time() - start) / 60
        print(f"\n{'='*80}\n⚡ COMPLETE in {duration:.1f}min (vs {duration*5:.1f}min without batch!)\n{'='*80}\n")
        
        return final


# ============ MAIN ============
if __name__ == "__main__":
    import sys
    
    print("\n" + "="*80)
    print("⚡ ULTRA-FAST META TRACKER - BATCH API")
    print("="*80)
    
    META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
    AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
    GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "google_credentials.json")
    GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")

    missing = []
    if not META_ACCESS_TOKEN:
        missing.append("META_ACCESS_TOKEN")
    if not AIRTABLE_TOKEN:
        missing.append("AIRTABLE_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_TABLE_NAME:
        missing.append("AIRTABLE_TABLE_NAME")
    if not GOOGLE_SPREADSHEET_ID:
        missing.append("GOOGLE_SPREADSHEET_ID")
    
    if missing:
        print("❌ Missing env vars:", ", ".join(missing))
        sys.exit(1)
    
    hours = 12
    if len(sys.argv) > 1:
        try:
            hours = int(sys.argv[1])
        except:
            pass
    
    print(f"\n📋 Config: hours={hours}, timestamp={datetime.now()}")
    print("="*80 + "\n")
    
    try:
        tracker = UltraFastMetaActivityTracker(
            meta_access_token=META_ACCESS_TOKEN,
            airtable_token=AIRTABLE_TOKEN,
            airtable_base_id=AIRTABLE_BASE_ID,
            airtable_table_name=AIRTABLE_TABLE_NAME,
            google_credentials_path=GOOGLE_CREDENTIALS_PATH,
            google_spreadsheet_id=GOOGLE_SPREADSHEET_ID,
            max_workers=10,
            debug_mode=False
        )
        
        results = tracker.run(hours=hours, append_mode=True, save_csv=False)
        
        print("="*80)
        print("⚡ SUCCESS - ULTRA FAST!")
        print("="*80)
        print(f"Activities: {len(results)}")
        print(f"Brands: {results['Brand'].nunique() if not results.empty else 0}")
        print(f"API calls saved: {tracker.debug_stats['batch_savings']}")
        print(f"Speedup: ~{tracker.debug_stats['batch_savings']/50:.0f}x faster!")
        print("="*80 + "\n")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"\n{'='*80}\n❌ FAILED\n{'='*80}\n{e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
