"""
betika_render_scraper.py
Single service that handles both daily scrape and live monitoring
Optimized for Render free tier (sleeps after inactivity)
"""

import time
import re
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import hashlib
import requests
from flask import Flask, jsonify

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient, UpdateOne

print("=" * 70)
print("⚽ BETIKA RENDER SCRAPER - OPTIMIZED FOR FREE TIER")
print("=" * 70)

# Flask app for web service
app = Flask(__name__)

DATABASE_URL = "mongodb+srv://Capiyo:Capiyo%401010@cluster0.22lay5z.mongodb.net/clashdb?retryWrites=true&w=majority&appName=Cluster0"


class RenderBetikaScraper:
    """Single service optimized for Render free tier"""
    
    def __init__(self):
        self.driver = None
        self.db = None
        self.games_collection = None
        self.stats_collection = None
        self.running = False
        self.last_daily_scrape = None
        self.setup()
    
    def setup(self):
        """Setup connections"""
        print("\n🔧 Setting up Render scraper...")
        self.connect_to_mongodb()
    
    def connect_to_mongodb(self):
        """Connect to MongoDB"""
        try:
            print("📡 Connecting to MongoDB...")
            client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            
            self.db = client.get_database()
            self.games_collection = self.db['games']
            self.stats_collection = self.db['game_stats']
            
            print(f"✅ Connected to database: {self.db.name}")
            return True
            
        except Exception as e:
            print(f"❌ MongoDB Connection Failed: {e}")
            return False
    
    def init_webdriver(self):
        """Initialize Selenium WebDriver (only when needed)"""
        if self.driver:
            return self.driver
        
        print("🚗 Initializing WebDriver...")
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
        return self.driver
    
    def close_webdriver(self):
        """Close WebDriver to save resources"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                print("💾 WebDriver closed to save resources")
            except:
                pass
    
    def check_daily_scrape_needed(self):
        """Check if daily scrape is needed (around 4 AM)"""
        now = datetime.now()
        
        # If we haven't done daily scrape today and it's around 4 AM
        if self.last_daily_scrape and self.last_daily_scrape.date() == now.date():
            return False  # Already scraped today
        
        # Run daily scrape between 4:00 AM and 5:00 AM
        if 4 <= now.hour <= 5:
            return True
        
        return False
    
    def run_daily_scrape_if_needed(self):
        """Run daily scrape if needed"""
        if self.check_daily_scrape_needed():
            print(f"\n🌅 Running daily scrape at {datetime.now().strftime('%H:%M')}")
            self.daily_scrape()
            self.last_daily_scrape = datetime.now()
            return True
        return False
    
    def daily_scrape(self):
        """Scrape all games for the day"""
        try:
            driver = self.init_webdriver()
            
            print("📥 Loading Betika for daily scrape...")
            url = "https://www.betika.com/en-ke/s/soccer"
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "prebet-match"))
            )
            time.sleep(3)
            
            matches = driver.find_elements(By.CLASS_NAME, "prebet-match")
            print(f"🎯 Found {len(matches)} matches")
            
            # Clear old games
            self.games_collection.delete_many({})
            
            games_data = []
            for i, match in enumerate(matches, 1):
                game = self.parse_match_simple(match, i)
                if game:
                    games_data.append(game)
            
            if games_data:
                # Calculate next check times
                for game in games_data:
                    game['next_check'] = self.calculate_next_check(game)
                    game['last_checked'] = None
                
                # Save to database
                self.games_collection.insert_many(games_data)
                print(f"✅ Saved {len(games_data)} games")
                
                # Show summary
                live_count = sum(1 for g in games_data if g.get('is_live', False))
                print(f"📊 Summary: {len(games_data)} total, {live_count} live")
            else:
                print("⚠️ No games found")
            
            # Close WebDriver to save resources
            self.close_webdriver()
            
        except Exception as e:
            print(f"❌ Error in daily scrape: {e}")
            self.close_webdriver()
    
    def parse_match_simple(self, match_element, index: int) -> Optional[Dict]:
        """Simple match parsing (you need to adjust based on actual structure)"""
        try:
            text = match_element.text.strip()
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            if len(lines) < 4:
                return None
            
            # Extract basic info
            home_team, away_team = self.extract_teams_simple(lines)
            if home_team == "Unknown":
                return None
            
            league = self.extract_league_simple(lines)
            match_date, match_time = self.extract_datetime_simple(lines)
            odds = self.extract_odds_simple(lines)
            
            # Check if live
            is_live = "LIVE" in text.upper()
            
            # Generate match ID
            match_string = f"{home_team}_{away_team}_{match_date}".lower().replace(" ", "_")
            match_id = hashlib.md5(match_string.encode()).hexdigest()[:12]
            
            return {
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "league": league,
                "date": match_date,
                "time": match_time,
                "home_win": odds[0],
                "draw": odds[1],
                "away_win": odds[2],
                "status": "live" if is_live else "upcoming",
                "is_live": is_live,
                "scraped_at": datetime.utcnow(),
                "home_score": 0 if is_live else None,
                "away_score": 0 if is_live else None
            }
            
        except Exception as e:
            print(f"  ⚠️ Error parsing match {index}: {e}")
            return None
    
    def extract_teams_simple(self, lines: List[str]) -> tuple:
        """Simple team extraction - ADJUST THIS BASED ON ACTUAL STRUCTURE"""
        home_team = "Unknown"
        away_team = "Unknown"
        
        # This is a placeholder - you need to implement actual parsing
        # Look for two substantial lines that aren't league/date/odds
        candidates = []
        for line in lines:
            # Skip obvious non-teams
            if (re.match(r'^\d+\.\d+$', line) or  # Odds
                re.match(r'^\d{1,2}/\d{1,2},\s*\d{1,2}:\d{2}$', line) or  # Date/time
                any(word in line.lower() for word in ['league', 'cup', 'premier', 'champions'])):
                continue
            
            if len(line) >= 3:
                candidates.append(line)
        
        if len(candidates) >= 2:
            home_team = candidates[0][:50]  # Limit length
            away_team = candidates[1][:50]
        elif len(candidates) == 1:
            home_team = candidates[0][:50]
            away_team = "Unknown Team"
        
        return home_team, away_team
    
    def extract_league_simple(self, lines: List[str]) -> str:
        """Extract league name"""
        for line in lines:
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in ['premier', 'league', 'cup', 'champions', 'uefa']):
                return line[:100]  # Limit length
        return "Unknown League"
    
    def extract_datetime_simple(self, lines: List[str]) -> tuple:
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
    
    def extract_odds_simple(self, lines: List[str]) -> tuple:
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
    
    def calculate_next_check(self, game: Dict) -> datetime:
        """Calculate when to next check this game"""
        now = datetime.utcnow()
        
        if game.get('is_live', False):
            return now + timedelta(minutes=2)  # Check live games every 2 min
        
        if game['time'] == 'TBD':
            return now + timedelta(minutes=30)  # Check TBD games every 30 min
        
        try:
            # Parse game time
            hour, minute = map(int, game['time'].split(':'))
            day, month = map(int, game['date'].split('/'))
            
            # Create datetime object (assuming current year)
            game_year = datetime.now().year
            game_datetime = datetime(game_year, month, day, hour, minute)
            
            if game_datetime > now:
                # Check 5 minutes before start
                return game_datetime - timedelta(minutes=5)
            else:
                # Should have started, check soon
                return now + timedelta(minutes=5)
                
        except:
            return now + timedelta(minutes=30)
    
    def check_games_status(self):
        """Check and update game statuses"""
        try:
            now = datetime.utcnow()
            
            # Find games that need checking
            games_to_check = list(self.games_collection.find({
                "$or": [
                    {"next_check": {"$lte": now}},
                    {"status": "live"}
                ]
            }))
            
            if not games_to_check:
                return
            
            print(f"🔍 Checking {len(games_to_check)} game(s)...")
            
            updates = []
            for game in games_to_check:
                update = self.update_game_status_logic(game, now)
                if update:
                    updates.append(update)
            
            # Bulk update database
            if updates:
                self.games_collection.bulk_write(updates)
                print(f"✅ Updated {len(updates)} game(s)")
            
            # Update live game stats
            self.update_live_game_stats()
            
        except Exception as e:
            print(f"⚠️ Error checking games: {e}")
    
    def update_game_status_logic(self, game: Dict, now: datetime) -> Optional[UpdateOne]:
        """Logic to update game status"""
        match_id = game['match_id']
        
        # If game is live, keep it live
        if game['status'] == 'live':
            # Check if game should be marked as completed
            if self.should_game_be_completed(game):
                return UpdateOne(
                    {"match_id": match_id},
                    {"$set": {
                        "status": "completed",
                        "is_live": False,
                        "completed_at": now,
                        "next_check": now + timedelta(days=1)  # Check tomorrow
                    }}
                )
            else:
                # Keep checking live game
                return UpdateOne(
                    {"match_id": match_id},
                    {"$set": {
                        "next_check": now + timedelta(minutes=2),
                        "last_checked": now
                    }}
                )
        
        # If game is upcoming, check if it should be live
        elif game['status'] == 'upcoming':
            if self.should_game_be_live(game):
                return UpdateOne(
                    {"match_id": match_id},
                    {"$set": {
                        "status": "live",
                        "is_live": True,
                        "next_check": now + timedelta(minutes=2),
                        "last_checked": now,
                        "home_score": 0,
                        "away_score": 0
                    }}
                )
            else:
                # Update next check time
                next_check = self.calculate_next_check(game)
                return UpdateOne(
                    {"match_id": match_id},
                    {"$set": {
                        "next_check": next_check,
                        "last_checked": now
                    }}
                )
        
        return None
    
    def should_game_be_live(self, game: Dict) -> bool:
        """Check if upcoming game should be live"""
        if game['time'] == 'TBD':
            return False
        
        try:
            # Parse game time
            hour, minute = map(int, game['time'].split(':'))
            day, month = map(int, game['date'].split('/'))
            
            game_year = datetime.now().year
            game_datetime = datetime(game_year, month, day, hour, minute)
            
            # Game is live if it started within the last 2 hours
            time_diff = datetime.now() - game_datetime
            return timedelta(minutes=-5) <= time_diff <= timedelta(hours=2)
            
        except:
            return False
    
    def should_game_be_completed(self, game: Dict) -> bool:
        """Check if live game should be marked as completed"""
        # If game has been live for more than 3 hours, mark as completed
        if game.get('last_checked'):
            time_since_check = datetime.utcnow() - game['last_checked']
            return time_since_check > timedelta(hours=3)
        
        return False
    
    def update_live_game_stats(self):
        """Update stats for live games (simulated for now)"""
        live_games = list(self.games_collection.find({"status": "live"}))
        
        if not live_games:
            return
        
        print(f"📊 Updating stats for {len(live_games)} live game(s)")
        
        stats_data = []
        for game in live_games:
            # Simulate stats update (in reality, scrape from Betika)
            stats = {
                "match_id": game['match_id'],
                "timestamp": datetime.utcnow(),
                "home_score": game.get('home_score', 0),
                "away_score": game.get('away_score', 0),
                "possession_home": 50,  # Replace with actual scraping
                "possession_away": 50,
                "shots_home": 5,
                "shots_away": 3,
                "corners_home": 2,
                "corners_away": 1
            }
            stats_data.append(stats)
        
        # Save stats
        if stats_data:
            self.stats_collection.insert_many(stats_data)
    
    def run_scheduler(self):
        """Main scheduler loop"""
        self.running = True
        
        print(f"\n🚀 Starting Render scraper scheduler")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Initial daily scrape if needed
        self.run_daily_scrape_if_needed()
        
        iteration = 0
        try:
            while self.running:
                iteration += 1
                
                # Run every 5 minutes
                if iteration % 5 == 0:
                    print(f"\n🔄 Iteration #{iteration}")
                    print(f"⏰ {datetime.now().strftime('%H:%M:%S')}")
                    
                    # Check if daily scrape is needed
                    self.run_daily_scrape_if_needed()
                    
                    # Check game statuses
                    self.check_games_status()
                    
                    # Show stats
                    self.show_current_stats()
                
                # Sleep for 1 minute
                time.sleep(60)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping scheduler...")
        except Exception as e:
            print(f"\n❌ Scheduler error: {e}")
        finally:
            self.close_webdriver()
    
    def show_current_stats(self):
        """Show current statistics"""
        try:
            total = self.games_collection.count_documents({})
            live = self.games_collection.count_documents({"status": "live"})
            upcoming = self.games_collection.count_documents({"status": "upcoming"})
            completed = self.games_collection.count_documents({"status": "completed"})
            
            print(f"📊 Stats: {total} total, {live} live, {upcoming} upcoming, {completed} completed")
            
        except Exception as e:
            print(f"⚠️ Error showing stats: {e}")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False


# Create global scraper instance
scraper = RenderBetikaScraper()

# Flask routes for web service
@app.route('/')
def home():
    """Home page - shows scraper status"""
    return jsonify({
        "status": "running",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "service": "Betika Scraper"
    })

@app.route('/stats')
def stats():
    """Get current statistics"""
    try:
        total = scraper.games_collection.count_documents({})
        live = scraper.games_collection.count_documents({"status": "live"})
        
        return jsonify({
            "total_games": total,
            "live_games": live,
            "last_updated": datetime.now().isoformat()
        })
    except:
        return jsonify({"error": "Unable to fetch stats"})

@app.route('/live-games')
def live_games():
    """Get current live games"""
    try:
        games = list(scraper.games_collection.find(
            {"status": "live"},
            {"_id": 0, "home_team": 1, "away_team": 1, "home_score": 1, "away_score": 1, "time": 1}
        ).limit(10))
        
        return jsonify({
            "live_games": games,
            "count": len(games)
        })
    except:
        return jsonify({"error": "Unable to fetch live games"})

@app.route('/force-daily-scrape')
def force_daily_scrape():
    """Force daily scrape (for testing)"""
    scraper.run_daily_scrape_if_needed()
    return jsonify({"message": "Daily scrape triggered"})

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})


def run_flask():
    """Run Flask web server"""
    print("🌐 Starting Flask web server...")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)


def main():
    """Main function"""
    print(f"\n🚀 BETIKA SCRAPER FOR RENDER")
    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d')}")
    
    # Check database connection
    if not scraper.connect_to_mongodb():
        print("❌ Failed to connect to database")
        return
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Start scheduler in main thread
    try:
        scraper.run_scheduler()
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.stop()
        print("\n✅ Scraper stopped")


if __name__ == "__main__":
    main()