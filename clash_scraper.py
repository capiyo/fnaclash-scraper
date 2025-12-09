"""
betika_daily_scraper.py
Runs at 4 AM daily to get all games for the day
"""

import time
import re
from datetime import datetime, timedelta
from typing import List, Dict
import hashlib

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient

print("=" * 70)
print("🌅 BETIKA DAILY 4AM SCRAPER")
print("=" * 70)

DATABASE_URL = "mongodb+srv://Capiyo:Capiyo%401010@cluster0.22lay5z.mongodb.net/clashdb?retryWrites=true&w=majority&appName=Cluster0"


class DailyBetikaScraper:
    """Runs once daily at 4 AM to get all games"""
    
    def __init__(self):
        self.driver = None
        self.db = None
        self.games_collection = None
        self.setup()
    
    def setup(self):
        """Setup WebDriver and Database"""
        print("\n🔧 Setting up daily scraper...")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(30)
        
        print("✅ WebDriver ready")
        self.connect_to_mongodb()
    
    def connect_to_mongodb(self):
        """Connect to MongoDB"""
        try:
            print("📡 Connecting to MongoDB...")
            client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            
            self.db = client.get_database()
            self.games_collection = self.db['games']
            
            print(f"✅ Connected to database: {self.db.name}")
            print(f"✅ Using collection: {self.games_collection.name}")
            return True
            
        except Exception as e:
            print(f"❌ MongoDB Connection Failed: {e}")
            self.db = None
            self.games_collection = None
            return False
    
    def run_daily_scrape(self):
        """Main function to run daily scrape"""
        print(f"\n📅 Daily scrape started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Clear old games
            result = self.games_collection.delete_many({})
            print(f"🗑️  Cleared {result.deleted_count} old games")
            
            # Scrape all games
            games = self.scrape_all_games()
            
            if games:
                # Save games with next_check_time
                for game in games:
                    game['next_check_time'] = self.calculate_next_check_time(game['time'])
                    game['last_checked'] = None
                    game['check_count'] = 0
                
                # Insert all games
                inserted = self.games_collection.insert_many(games)
                print(f"✅ Saved {len(inserted.inserted_ids)} games for today")
                
                # Show summary
                self.show_summary(games)
            else:
                print("⚠️ No games found for today")
            
            return True
            
        except Exception as e:
            print(f"❌ Error in daily scrape: {e}")
            return False
        finally:
            self.close()
    
    def scrape_all_games(self) -> List[Dict]:
        """Scrape all games from Betika"""
        print("📥 Loading Betika games...")
        
        try:
            url = "https://www.betika.com/en-ke/s/soccer"
            self.driver.get(url)
            
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "prebet-match"))
            )
            time.sleep(3)
            
            matches = self.driver.find_elements(By.CLASS_NAME, "prebet-match")
            print(f"🎯 Found {len(matches)} matches")
            
            games = []
            for i, match in enumerate(matches, 1):
                game = self.parse_match(match, i)
                if game:
                    games.append(game)
            
            return games
            
        except Exception as e:
            print(f"❌ Error scraping: {e}")
            return []
    
    def parse_match(self, match_element, index: int) -> Optional[Dict]:
        """Parse a single match"""
        try:
            text = match_element.text.strip()
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            if len(lines) < 4:
                return None
            
            # Extract teams (you need to adjust this based on actual structure)
            home_team, away_team = self.extract_teams(lines)
            if home_team == "Unknown" or away_team == "Unknown":
                return None
            
            # Extract other info
            league = self.extract_league(lines)
            match_date, match_time = self.extract_datetime(lines)
            home_odds, draw_odds, away_odds = self.extract_odds(lines)
            
            # Determine status
            status = "live" if "LIVE" in text.upper() else "upcoming"
            
            # Generate match ID
            match_id = hashlib.md5(
                f"{home_team}_{away_team}_{match_date}".lower().replace(" ", "_").encode()
            ).hexdigest()[:12]
            
            return {
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "league": league,
                "date": match_date,
                "time": match_time,
                "home_win": home_odds,
                "draw": draw_odds,
                "away_win": away_odds,
                "status": status,
                "is_live": status == "live",
                "scraped_at": datetime.utcnow()
            }
            
        except Exception as e:
            print(f"  ⚠️ Error parsing match {index}: {e}")
            return None
    
    def extract_teams(self, lines: List[str]) -> tuple:
        """Extract team names"""
        # TODO: Implement proper team extraction based on Betika's structure
        home_team = "Team A"
        away_team = "Team B"
        
        # Simple logic - adjust as needed
        candidates = []
        for line in lines:
            if (not re.match(r'^\d+\.\d+$', line) and
                not re.match(r'^\d{1,2}/\d{1,2},\s*\d{1,2}:\d{2}$', line) and
                not any(word in line.lower() for word in ['league', 'cup', 'premier']) and
                len(line) >= 3):
                candidates.append(line)
        
        if len(candidates) >= 2:
            home_team = candidates[0]
            away_team = candidates[1]
        
        return home_team, away_team
    
    def extract_league(self, lines: List[str]) -> str:
        """Extract league name"""
        for line in lines:
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in ['premier', 'league', 'cup', 'champions']):
                return line
        return "Unknown League"
    
    def extract_datetime(self, lines: List[str]) -> tuple:
        """Extract date and time"""
        today = datetime.now().strftime("%d/%m")
        match_date = today
        match_time = "TBD"
        
        for line in lines:
            match = re.search(r'(\d{1,2}/\d{1,2})\s*,\s*(\d{1,2}:\d{2})', line)
            if match:
                match_date = match.group(1)
                match_time = match.group(2)
                break
            
            if re.match(r'^\d{1,2}:\d{2}$', line):
                match_time = line
        
        return match_date, match_time
    
    def extract_odds(self, lines: List[str]) -> tuple:
        """Extract odds"""
        odds = []
        for line in lines:
            if re.match(r'^\d+\.\d{1,2}$', line):
                try:
                    odd = float(line)
                    if 1.0 <= odd <= 100.0:
                        odds.append(odd)
                except:
                    continue
        
        if len(odds) >= 3:
            return odds[0], odds[1], odds[2]
        elif len(odds) == 2:
            return odds[0], 0.0, odds[1]
        elif len(odds) == 1:
            return odds[0], 0.0, 0.0
        else:
            return 0.0, 0.0, 0.0
    
    def calculate_next_check_time(self, match_time: str) -> datetime:
        """Calculate when to next check this game"""
        now = datetime.utcnow()
        
        if match_time == "TBD" or match_time == "LIVE":
            return now + timedelta(minutes=30)  # Check again in 30 minutes
        
        try:
            # Parse match time
            hour, minute = map(int, match_time.split(':'))
            match_datetime = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # If match already started, check soon
            if match_datetime <= now:
                return now + timedelta(minutes=5)
            
            # If match is later today, check 5 minutes before start
            return match_datetime - timedelta(minutes=5)
            
        except:
            return now + timedelta(minutes=30)
    
    def show_summary(self, games: List[Dict]):
        """Show summary of scraped games"""
        print(f"\n📊 DAILY SCRAPE SUMMARY:")
        print(f"   Total games: {len(games)}")
        
        live_count = sum(1 for g in games if g['status'] == 'live')
        upcoming_count = len(games) - live_count
        
        print(f"   🔥 Live games: {live_count}")
        print(f"   ⏳ Upcoming games: {upcoming_count}")
        
        if games:
            print(f"\n📋 First 3 games:")
            for i, game in enumerate(games[:3], 1):
                print(f"   {i}. {game['home_team']} vs {game['away_team']}")
                print(f"      Time: {game['time']}, League: {game['league']}")
    
    def close(self):
        """Close connections"""
        if self.driver:
            try:
                self.driver.quit()
                print("✅ WebDriver closed")
            except:
                pass
        print("✅ Daily scraper finished")


def main():
    """Main function for daily scraper"""
    print(f"\n⏰ Starting daily 4 AM scrape...")
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d')}")
    
    scraper = DailyBetikaScraper()
    
    try:
        if not scraper.connect_to_mongodb():
            print("❌ Failed to connect to database")
            return
        
        success = scraper.run_daily_scrape()
        
        if success:
            print(f"\n🎉 Daily scrape completed successfully!")
        else:
            print(f"\n⚠️ Daily scrape completed with errors")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ Daily scrape process completed")


if __name__ == "__main__":
    main()