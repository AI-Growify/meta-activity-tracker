import os
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

load_dotenv()


class MetaActivityTrackerWithAirtable:
    """Meta activity tracker with Airtable brand/manager mapping"""
    
    def __init__(self,
                 meta_access_token,
                 airtable_token,
                 airtable_base_id,
                 airtable_table_name,
                 google_credentials_path=None,
                 google_spreadsheet_id=None,
                 max_workers=5):
        
        # Meta API
        self.meta_access_token = meta_access_token
        self.meta_base_url = "https://graph.facebook.com/v18.0"
        self.session = self._create_session_with_retries()
        
        # Airtable API
        self.airtable_token = airtable_token
        self.airtable_base_id = airtable_base_id
        self.airtable_table_name = airtable_table_name
        self.airtable_url = f'https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}'
        
        # Google Sheets
        self.google_credentials_path = google_credentials_path
        self.google_spreadsheet_id = google_spreadsheet_id
        self.gspread_client = self.setup_google_sheets() if google_credentials_path else None
        
        self.max_workers = max_workers
        self.brand_mapping_df = None
        self.brand_mapping_dict = {}

    def _create_session_with_retries(self):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _make_api_request(self, url, params=None, headers=None, timeout=10):
        try:
            r = self.session.get(url, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"⚠️ API request failed: {e}")
            return None

    def _normalize_brand_name(self, name):
        """Normalize brand name for matching"""
        if pd.isna(name) or not name:
            return ''
        
        name = str(name).lower().strip()
        
        remove_terms = [
            'pvt ltd', 'private limited', 'pvt. ltd.', 'private ltd',
            'llp', 'opc', 'limited', 'ltd', 'inc', 'corp',
            '- current', '- new', '- old', 'domestic', 'export',
            'the ', 'a ', 'an '
        ]
        
        for term in remove_terms:
            name = name.replace(term, '')
        
        name = ' '.join(name.split())
        name = ''.join(c for c in name if c.isalnum() or c.isspace())
        
        return name.strip()

    def _find_best_brand_match(self, brand_name):
        """Find best matching brand from Airtable using fuzzy logic"""
        if not brand_name:
            return None
        
        normalized_input = self._normalize_brand_name(brand_name)
        
        if normalized_input in self.brand_mapping_dict:
            return self.brand_mapping_dict[normalized_input]
        
        for airtable_brand_normalized, mapping_data in self.brand_mapping_dict.items():
            if (normalized_input in airtable_brand_normalized or 
                airtable_brand_normalized in normalized_input):
                
                if len(normalized_input) >= 5 and len(airtable_brand_normalized) >= 5:
                    return mapping_data
        
        return None

    def fetch_airtable_data(self):
        """Fetch all brand/manager mapping data from Airtable"""
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
            if offset:
                url = f"{self.airtable_url}?offset={offset}"
            else:
                url = None
            
            time.sleep(0.1)
        
        if not all_records:
            print("❌ No records found in Airtable")
            return pd.DataFrame()
        
        df = pd.DataFrame([record.get('fields', {}) for record in all_records])
        
        if df.empty:
            print("❌ Airtable DataFrame is empty")
            return pd.DataFrame()
        
        df.columns = df.columns.str.strip()
        
        print(f"✅ Fetched {len(df)} brands from Airtable")
        print(f"   Columns: {', '.join(df.columns.tolist())}")
        
        return df

    def get_all_ad_accounts(self):
        """Get all Meta ad accounts"""
        url = f"{self.meta_base_url}/me/adaccounts"
        params = {
            'access_token': self.meta_access_token,
            'fields': 'id,name,account_status,business_name,currency,timezone_name',
            'limit': 100
        }
        
        accounts = []
        while True:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break
            
            accounts.extend(data.get('data', []))
            
            paging = data.get('paging', {})
            next_url = paging.get('next')
            if not next_url:
                break
            url = next_url
            params = {}
            time.sleep(0.05)
        
        print(f"✅ Found {len(accounts)} Meta ad accounts")
        return accounts

    def get_account_activities(self, ad_account_id, hours=24):
        """Get activities for an account from last N hours"""
        since_dt = datetime.now() - timedelta(hours=hours)
        since_iso = since_dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        url = f"{self.meta_base_url}/{ad_account_id}/activities"
        params = {
            'access_token': self.meta_access_token,
            'since': since_iso,
            'limit': 500,
            'fields': 'event_type,event_time,actor_name,object_name,object_type,object_id,translated_event_type'
        }
        
        activities = []
        while True:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break
            
            activities.extend(data.get('data', []))
            
            paging = data.get('paging', {})
            next_url = paging.get('next')
            if not next_url:
                break
            url = next_url
            params = {}
            time.sleep(0.03)
        
        return activities

    def get_campaigns_for_account(self, ad_account_id):
        """Get campaigns to extract brand info"""
        url = f"{self.meta_base_url}/{ad_account_id}/campaigns"
        params = {
            'access_token': self.meta_access_token,
            'fields': 'id,name,status,effective_status,objective,created_time,updated_time',
            'limit': 500
        }
        
        campaigns = []
        while True:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break
            
            campaigns.extend(data.get('data', []))
            
            paging = data.get('paging', {})
            next_url = paging.get('next')
            if not next_url:
                break
            url = next_url
            params = {}
            time.sleep(0.03)
        
        return campaigns

    def _process_account(self, account, hours=24):
        """Process one account - get activities and basic info"""
        account_id = account.get('id')
        account_name = account.get('name', 'Unknown')
        business_name = account.get('business_name', '')
        
        brand = business_name if business_name else account_name
        
        activities = self.get_account_activities(account_id, hours) or []
        
        if not activities:
            return []
        
        campaigns = self.get_campaigns_for_account(account_id) or []
        campaign_lookup = {c['id']: c for c in campaigns}
        
        results = []
        for activity in activities:
            object_id = activity.get('object_id', '')
            object_name = activity.get('object_name', '')
            object_type = activity.get('object_type', '')
            
            campaign_name = ''
            campaign_status = ''
            campaign_objective = ''
            
            if object_type == 'campaign' and object_id in campaign_lookup:
                camp = campaign_lookup[object_id]
                campaign_name = camp.get('name', object_name)
                campaign_status = camp.get('effective_status', camp.get('status', ''))
                campaign_objective = camp.get('objective', '')
            else:
                campaign_name = object_name
            
            timestamp = activity.get('event_time', '')
            timestamp_parsed = ''
            if timestamp:
                try:
                    dt = datetime.strptime(timestamp.replace('Z', '+00:00').split('+')[0], '%Y-%m-%dT%H:%M:%S')
                    timestamp_parsed = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    timestamp_parsed = timestamp
            
            results.append({
                'Brand': brand,
                'Account_ID': account_id,
                'Account_Name': account_name,
                'Actor': activity.get('actor_name', 'Unknown'),
                'Action': activity.get('translated_event_type', activity.get('event_type', 'Unknown')),
                'Object_Name': campaign_name,
                'Object_Type': object_type,
                'Campaign_Status': campaign_status,
                'Campaign_Objective': campaign_objective,
                'Timestamp': timestamp_parsed,
                'Raw_Event_Type': activity.get('event_type', '')
            })
        
        return results

    def fetch_meta_activities(self, hours=24):
        """Fetch all activities from all accounts in parallel"""
        print("\n" + "="*80)
        print(f"FETCHING META ACTIVITIES (Last {hours} hours)")
        print("="*80)
        
        accounts = self.get_all_ad_accounts()
        if not accounts:
            print("❌ No Meta accounts found")
            return pd.DataFrame()
        
        all_activities = []
        
        print(f"\nProcessing {len(accounts)} accounts with {self.max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_account, acc, hours): acc
                for acc in accounts
            }
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 10 == 0:
                    print(f"  Progress: {completed}/{len(accounts)} accounts processed")
                
                try:
                    activities = future.result()
                    all_activities.extend(activities)
                except Exception as e:
                    print(f"⚠️ Error processing account: {e}")
        
        print(f"\n✅ Processing complete!")
        
        if not all_activities:
            print("ℹ️ No activities found in the specified time period")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_activities)
        
        if 'Timestamp' in df.columns:
            df = df.sort_values('Timestamp', ascending=False)
        
        return df

    def map_airtable_to_activities(self, activities_df):
        """Map Airtable brand data to Meta activities with fuzzy matching"""
        print("\n" + "="*80)
        print("MAPPING AIRTABLE DATA TO ACTIVITIES")
        print("="*80)
        
        if activities_df.empty:
            print("⚠️ No activities to map")
            return activities_df
        
        if self.brand_mapping_df is None or self.brand_mapping_df.empty:
            print("⚠️ No Airtable data available for mapping")
            return activities_df
        
        possible_brand_cols = ['Brand', 'Brands', 'Brand Name', 'brand', 'brands']
        possible_fb_manager_cols = ['FB Manager', 'FB_Manager', 'Facebook Manager', 'fb_manager']
        possible_brand_manager_cols = ['Brand Manager', 'Brand_Manager', 'brand_manager']
        possible_team_cols = ['Current Team', 'Team', 'Current_Team', 'team']
        
        brand_col = next((col for col in possible_brand_cols if col in self.brand_mapping_df.columns), None)
        fb_manager_col = next((col for col in possible_fb_manager_cols if col in self.brand_mapping_df.columns), None)
        brand_manager_col = next((col for col in possible_brand_manager_cols if col in self.brand_mapping_df.columns), None)
        team_col = next((col for col in possible_team_cols if col in self.brand_mapping_df.columns), None)
        
        print(f"   Airtable columns found:")
        print(f"   - Brand column: {brand_col}")
        print(f"   - FB Manager: {fb_manager_col}")
        print(f"   - Brand Manager: {brand_manager_col}")
        print(f"   - Team: {team_col}")
        
        if not brand_col:
            print("❌ Could not find Brand column in Airtable data!")
            return activities_df
        
        print("\n   Building fuzzy matching dictionary...")
        for _, row in self.brand_mapping_df.iterrows():
            brand_name = str(row[brand_col]).strip() if pd.notna(row[brand_col]) else ''
            if brand_name:
                normalized = self._normalize_brand_name(brand_name)
                if normalized:
                    self.brand_mapping_dict[normalized] = {
                        'original_name': brand_name,
                        'FB_Manager': row[fb_manager_col] if fb_manager_col and pd.notna(row[fb_manager_col]) else 'Not Assigned',
                        'Brand_Manager': row[brand_manager_col] if brand_manager_col and pd.notna(row[brand_manager_col]) else 'Not Assigned',
                        'Current_Team': row[team_col] if team_col and pd.notna(row[team_col]) else 'Not Assigned'
                    }
        
        print(f"   Created {len(self.brand_mapping_dict)} normalized brand mappings")
        
        def map_brand_data(brand_name):
            match = self._find_best_brand_match(brand_name)
            if match:
                return pd.Series({
                    'FB_Manager': match['FB_Manager'],
                    'Brand_Manager': match['Brand_Manager'],
                    'Current_Team': match['Current_Team'],
                    'Matched_Airtable_Brand': match['original_name']
                })
            else:
                return pd.Series({
                    'FB_Manager': 'Unknown',
                    'Brand_Manager': 'Unknown',
                    'Current_Team': 'Unknown',
                    'Matched_Airtable_Brand': ''
                })
        
        print("\n   Applying fuzzy matching...")
        mapping_results = activities_df['Brand'].apply(map_brand_data)
        activities_df = pd.concat([activities_df, mapping_results], axis=1)
        
        activities_df['Fetch_Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        mapped_count = activities_df[activities_df['FB_Manager'] != 'Unknown'].shape[0]
        unmapped_count = activities_df[activities_df['FB_Manager'] == 'Unknown'].shape[0]
        
        print(f"\n   ✅ Mapped: {mapped_count} activities ({mapped_count/len(activities_df)*100:.1f}%)")
        print(f"   ⚠️ Unmapped: {unmapped_count} activities ({unmapped_count/len(activities_df)*100:.1f}%)")
        
        if unmapped_count > 0:
            unmapped_brands = activities_df[activities_df['FB_Manager'] == 'Unknown']['Brand'].unique()
            print(f"\n   Unmapped brands ({len(unmapped_brands)}):")
            for brand in unmapped_brands[:15]:
                print(f"      - {brand}")
            if len(unmapped_brands) > 15:
                print(f"      ... and {len(unmapped_brands) - 15} more")
        
        column_order = [
            'Brand', 'Matched_Airtable_Brand', 'FB_Manager', 'Brand_Manager', 'Current_Team',
            'Account_ID', 'Account_Name', 'Actor', 'Action',
            'Object_Name', 'Object_Type', 'Campaign_Status', 'Campaign_Objective',
            'Timestamp', 'Fetch_Date', 'Raw_Event_Type'
        ]
        
        column_order = [col for col in column_order if col in activities_df.columns]
        activities_df = activities_df[column_order]
        
        return activities_df

    # ============ NEW METHOD 1: SMART FETCH ============
    def get_last_entry_time_from_sheet(self):
        """Get the most recent timestamp from existing Google Sheet data"""
        if self.gspread_client is None:
            return None
        
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            ws = sh.worksheet('Meta_Activities_Log')
            
            data = ws.get_all_records()
            if not data:
                print("ℹ️ No existing data in sheet")
                return None
            
            df = pd.DataFrame(data)
            
            if 'Timestamp' not in df.columns:
                print("⚠️ No Timestamp column found")
                return None
            
            df = df[df['Timestamp'].notna() & (df['Timestamp'] != '')]
            if df.empty:
                print("⚠️ No valid timestamps found")
                return None
            
            df['Timestamp_dt'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            df = df.dropna(subset=['Timestamp_dt'])
            
            if df.empty:
                return None
            
            last_timestamp = df['Timestamp_dt'].max()
            print(f"📅 Last entry in sheet: {last_timestamp}")
            return last_timestamp
            
        except Exception as e:
            print(f"⚠️ Could not read last entry time: {e}")
            return None

    # ============ NEW METHOD 2: ACTIVITY LOGGER ============
    def log_github_activity(self, action, details):
        """Log activities to GitHub Actions Log sheet"""
        if self.gspread_client is None:
            return
        
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            
            try:
                ws = sh.worksheet('GitHub_Actions_Log')
            except:
                ws = sh.add_worksheet(title='GitHub_Actions_Log', rows=1000, cols=10)
                ws.append_row([
                    'Timestamp', 'Run Number', 'Action', 'Details', 
                    'Activities Count', 'Time Range', 'Status'
                ])
                ws.format('1:1', {
                    'textFormat': {'bold': True, 'fontSize': 11},
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2},
                    'horizontalAlignment': 'CENTER'
                })
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            run_number = os.getenv('GITHUB_RUN_NUMBER', 'manual')
            
            ws.append_row([
                timestamp,
                run_number,
                action,
                details,
                '',
                '',
                '✅ Success' if 'Success' in action or 'Completed' in action else '🔄 In Progress'
            ])
            
        except Exception as e:
            print(f"⚠️ Could not log to GitHub Actions sheet: {e}")

    def setup_google_sheets(self):
        if not (self.google_credentials_path and os.path.exists(self.google_credentials_path)):
            return None
        
        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            credentials = Credentials.from_service_account_file(
                self.google_credentials_path, scopes=scopes
            )
            return gspread.authorize(credentials)
        except Exception as e:
            print(f"⚠️ Could not setup Google Sheets: {e}")
            return None

    def read_existing_data_from_sheets(self, sheet_name='Meta_Activities_Log'):
        """Read existing data from Google Sheets"""
        if self.gspread_client is None:
            return pd.DataFrame()
        
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            ws = sh.worksheet(sheet_name)
            data = ws.get_all_records()
            
            if data:
                df = pd.DataFrame(data)
                print(f"📊 Found {len(df)} existing activities in Google Sheets")
                return df
            else:
                return pd.DataFrame()
        except:
            print("ℹ️ No existing data found in Google Sheets")
            return pd.DataFrame()

    def upload_to_sheets(self, df, sheet_name='Meta_Activities_Log', append_mode=False):
        if self.gspread_client is None:
            print("ℹ️ Skipping Google Sheets upload (no credentials)")
            return
        
        if df.empty:
            print("⚠️ No data to upload")
            return
        
        print(f"\n{'='*80}")
        print(f"UPLOADING TO GOOGLE SHEETS")
        print(f"{'='*80}")
        
        new_activities_count = 0
        
        try:
            sh = self.gspread_client.open_by_key(self.google_spreadsheet_id)
            
            if append_mode:
                existing_df = self.read_existing_data_from_sheets(sheet_name)
                
                if not existing_df.empty:
                    def create_unique_id(row):
                        return f"{row['Account_ID']}_{row['Object_Name']}_{row['Timestamp']}_{row['Action']}"
                    
                    existing_df['_unique_id'] = existing_df.apply(create_unique_id, axis=1)
                    df['_unique_id'] = df.apply(create_unique_id, axis=1)
                    
                    print(f"\n   Before deduplication:")
                    print(f"   - Existing: {len(existing_df)} rows")
                    print(f"   - New fetch: {len(df)} rows")
                    
                    existing_ids = set(existing_df['_unique_id'])
                    new_df = df[~df['_unique_id'].isin(existing_ids)].copy()
                    new_activities_count = len(new_df)
                    
                    print(f"   - Truly new activities: {new_activities_count} rows")
                    
                    if len(new_df) > 0:
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                        combined_df = combined_df.drop('_unique_id', axis=1)
                        combined_df = combined_df.sort_values('Timestamp', ascending=False)
                        print(f"   - Final total: {len(combined_df)} rows")
                        df = combined_df
                        
                        # Log new activities
                        if new_activities_count > 0:
                            newest_timestamp = new_df['Timestamp'].max()
                            oldest_timestamp = new_df['Timestamp'].min()
                            self.log_github_activity(
                                f'➕ Added {new_activities_count} New Activities',
                                f'Range: {oldest_timestamp} to {newest_timestamp}'
                            )
                    else:
                        print(f"   ℹ️ No new activities to add. Keeping existing data.")
                        existing_df = existing_df.drop('_unique_id', axis=1)
                        df = existing_df
            
            try:
                ws = sh.worksheet(sheet_name)
                ws.clear()
                print(f"✅ Cleared existing sheet '{sheet_name}'")
            except:
                ws = sh.add_worksheet(
                    title=sheet_name, 
                    rows=max(1000, len(df) + 50),
                    cols=max(20, len(df.columns) + 5)
                )
                print(f"✅ Created new sheet '{sheet_name}'")
            
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)
            
            ws.format('1:1', {
                'textFormat': {'bold': True, 'fontSize': 11},
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.2},
                'horizontalAlignment': 'CENTER'
            })
            
            ws.freeze(rows=1)
            
            print(f"✅ Uploaded {len(df)} activities to '{sheet_name}'")
            if new_activities_count > 0:
                print(f"   📊 {new_activities_count} NEW activities added")
            print(f"🔗 https://docs.google.com/spreadsheets/d/{self.google_spreadsheet_id}")
            
        except Exception as e:
            print(f"❌ Upload failed: {e}")

    # ============ UPDATED run() METHOD ============
    def run(self, hours=24, append_mode=False, save_csv=False):
        """Main execution pipeline with smart fetching"""
        start_time = time.time()
        
        print("="*80)
        print("META ACTIVITY TRACKER WITH AIRTABLE MAPPING")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Initial hours parameter: {hours}")
        print(f"Mode: {'APPEND' if append_mode else 'REPLACE'}")
        print("="*80)
        
        # SMART FETCH: Calculate actual hours needed
        if append_mode:
            last_entry_time = self.get_last_entry_time_from_sheet()
            
            if last_entry_time:
                now = datetime.now()
                hours_since_last = (now - last_entry_time).total_seconds() / 3600
                adjusted_hours = int(hours_since_last) + 2
                
                print(f"\n🔍 SMART FETCH CALCULATION:")
                print(f"   Last entry time: {last_entry_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   Hours since last entry: {hours_since_last:.2f}")
                print(f"   Adjusted fetch window: {adjusted_hours} hours (with 2h buffer)")
                print("="*80 + "\n")
                
                hours = adjusted_hours
                
                self.log_github_activity(
                    '🔍 Smart Fetch Calculated',
                    f'Last: {last_entry_time.strftime("%Y-%m-%d %H:%M:%S")}, '
                    f'Gap: {hours_since_last:.1f}h, Fetching: {adjusted_hours}h'
                )
            else:
                print(f"\nℹ️ No previous data found, using default: {hours} hours\n")
                self.log_github_activity('📋 First Run', f'Fetching last {hours} hours')
        
        self.log_github_activity('🚀 Tracker Started', f'Fetching last {hours} hours')
        
        self.brand_mapping_df = self.fetch_airtable_data()
        activities_df = self.fetch_meta_activities(hours=hours)
        
        if activities_df.empty:
            print("\n✅ Process complete - no activities found")
            self.log_github_activity('ℹ️ No New Activities', f'No activities in last {hours}h')
            return activities_df
        
        final_df = self.map_airtable_to_activities(activities_df)
        
        if 'Timestamp' in final_df.columns:
            time_range = f"{final_df['Timestamp'].min()} to {final_df['Timestamp'].max()}"
        else:
            time_range = 'N/A'
        
        print("\n" + "="*80)
        print("ACTIVITY SUMMARY")
        print("="*80)
        print(f"Total activities: {len(final_df)}")
        print(f"Unique brands: {final_df['Brand'].nunique()}")
        print(f"Unique actors: {final_df['Actor'].nunique()}")
        print(f"Time range: {time_range}")
        
        print("\n📊 Top 10 Most Active Brands:")
        print(final_df['Brand'].value_counts().head(10))
        
        print("\n📊 Top 10 Most Active People:")
        print(final_df['Actor'].value_counts().head(10))
        
        print("\n📊 Activity Types:")
        print(final_df['Action'].value_counts().head(10))
        
        if save_csv:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_file = f"meta_activities_{timestamp}.csv"
            final_df.to_csv(csv_file, index=False)
            print(f"\n💾 Saved to: {csv_file}")
        
        if self.gspread_client:
            self.upload_to_sheets(final_df, append_mode=append_mode)
            duration = (time.time() - start_time) / 60
            self.log_github_activity(
                '✅ Tracker Completed',
                f'{len(final_df)} activities in {duration:.1f}min. Range: {time_range}'
            )
        
        return final_df


# ============ MAIN ============
if __name__ == "__main__":
    import sys
    
    print("\n" + "="*80)
    print("🚀 META ACTIVITY TRACKER - GITHUB ACTIONS")
    print("="*80)
    
    # Get environment variables
    META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
    AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
    GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "google_credentials.json")
    GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")

    # Validate required variables
    missing_vars = []
    if not META_ACCESS_TOKEN:
        missing_vars.append("META_ACCESS_TOKEN")
    if not AIRTABLE_TOKEN:
        missing_vars.append("AIRTABLE_TOKEN")
    if not AIRTABLE_BASE_ID:
        missing_vars.append("AIRTABLE_BASE_ID")
    if not AIRTABLE_TABLE_NAME:
        missing_vars.append("AIRTABLE_TABLE_NAME")
    if not GOOGLE_SPREADSHEET_ID:
        missing_vars.append("GOOGLE_SPREADSHEET_ID")
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 Make sure all GitHub Secrets are added!")
        sys.exit(1)
    
    # Get hours from command line or default
    hours = 12
    if len(sys.argv) > 1:
        try:
            hours = int(sys.argv[1])
        except ValueError:
            print(f"⚠️ Invalid hours argument, using default: 12")
            hours = 12
    
    print(f"\n📋 Configuration:")
    print(f"   Hours to fetch: {hours}")
    print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Credentials: {GOOGLE_CREDENTIALS_PATH}")
    print(f"   Sheet ID: {GOOGLE_SPREADSHEET_ID[:20]}...")
    print("="*80 + "\n")
    
    try:
        # Create tracker instance
        tracker = MetaActivityTrackerWithAirtable(
            meta_access_token=META_ACCESS_TOKEN,
            airtable_token=AIRTABLE_TOKEN,
            airtable_base_id=AIRTABLE_BASE_ID,
            airtable_table_name=AIRTABLE_TABLE_NAME,
            google_credentials_path=GOOGLE_CREDENTIALS_PATH,
            google_spreadsheet_id=GOOGLE_SPREADSHEET_ID,
            max_workers=5
        )
        
        # Run tracker with SMART FETCH enabled (append_mode=True)
        results = tracker.run(hours=hours, append_mode=True, save_csv=False)
        
        # Success summary
        print("\n" + "="*80)
        print("✅ TRACKER COMPLETED SUCCESSFULLY! 🎉")
        print("="*80)
        print(f"   Activities processed: {len(results)}")
        print(f"   Unique brands: {results['Brand'].nunique() if not results.empty else 0}")
        print(f"   Data saved to Google Sheets")
        print("="*80 + "\n")
        
        sys.exit(0)
        
    except Exception as e:
        print("\n" + "="*80)
        print("❌ TRACKER FAILED")
        print("="*80)
        print(f"Error: {str(e)}")
        print("\nFull traceback:")
        import traceback
        traceback.print_exc()
        print("="*80 + "\n")
        sys.exit(1)
