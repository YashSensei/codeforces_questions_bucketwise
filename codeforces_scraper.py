#!/usr/bin/env python3
"""
Enhanced Codeforces Problem Scraper with Official API - JSON Output
Scrapes problems using Codeforces official API and saves each in JSON format.
Organizes problems into rating buckets and supports filtering by tags, difficulty ratings, and contest types.
"""

import requests
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import re
from typing import List, Dict, Optional, Set, Tuple
import logging
import argparse
import sys
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('codeforces_scraper.log'),
        logging.StreamHandler()
    ]
)

@dataclass
class ScrapingConfig:
    """Configuration class for scraping parameters"""
    output_dir: str = "codeforces_problems"
    max_workers: int = 10  # Reduced for better rate limiting
    tags_filter: Optional[List[str]] = None
    min_rating: Optional[int] = None
    max_rating: Optional[int] = None
    contest_types: Optional[List[str]] = None
    include_gym: bool = False
    language: str = "en"  # en or ru
    bucket_size: int = 400  # Rating bucket size

class EnhancedCodeforcesScraper:
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.base_url = "https://codeforces.com/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.lock = Lock()
        self.processed_count = 0
        self.rate_limit_delay = 2.5  # Increased delay for safety
        self.last_request_time = 0
        
        # Initialize statistics first
        self.stats = {
            'total_problems': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'contests_fetched': 0,
            'bucket_stats': {}
        }
        
        # Create output directory
        os.makedirs(config.output_dir, exist_ok=True)
        
        # Create rating bucket directories
        self.rating_buckets = self._create_rating_buckets()
        self._create_bucket_directories()
        
    def _create_rating_buckets(self) -> Dict[str, Tuple[Optional[int], Optional[int]]]:
        """Create rating bucket definitions"""
        buckets = {}
        
        # Standard buckets with safer ranges
        buckets['0800-1199'] = (800, 1199)
        buckets['1200-1399'] = (1200, 1399)
        buckets['1400-1599'] = (1400, 1599)
        buckets['1600-1899'] = (1600, 1899)
        buckets['1900-2099'] = (1900, 2099)
        buckets['2100-2299'] = (2100, 2299)
        buckets['2300-2499'] = (2300, 2499)
        buckets['2500-2699'] = (2500, 2699)
        buckets['2700-2999'] = (2700, 2999)
        buckets['3000-plus'] = (3000, None)  # Use None instead of inf
        buckets['unrated'] = (None, None)  # For unrated problems
        
        return buckets
    
    def _create_bucket_directories(self):
        """Create directories for each rating bucket"""
        for bucket_name in self.rating_buckets.keys():
            bucket_path = os.path.join(self.config.output_dir, bucket_name)
            os.makedirs(bucket_path, exist_ok=True)
            self.stats['bucket_stats'][bucket_name] = {
                'count': 0,
                'path': bucket_path
            }
    
    def _get_rating_bucket(self, rating: Optional[int]) -> str:
        """Determine which bucket a problem belongs to based on its rating"""
        if rating is None:
            return 'unrated'
        
        for bucket_name, (min_rating, max_rating) in self.rating_buckets.items():
            if bucket_name == 'unrated':
                continue
            
            if min_rating is not None and max_rating is not None:
                if min_rating <= rating <= max_rating:
                    return bucket_name
            elif min_rating is not None and max_rating is None:  # Handle "plus" buckets
                if rating >= min_rating:
                    return bucket_name
        
        return 'unrated'
        
    def rate_limit(self):
        """Ensure we don't exceed API rate limit (1 request per 2 seconds)"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def make_api_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make a rate-limited API request with better error handling"""
        self.rate_limit()
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}/{endpoint}"
                if params is None:
                    params = {}
                
                # Add language parameter
                params['lang'] = self.config.language
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if data['status'] != 'OK':
                    error_msg = data.get('comment', 'Unknown error')
                    if 'Call limit exceeded' in error_msg:
                        logging.warning(f"Rate limit hit, waiting longer...")
                        time.sleep(5)  # Wait longer on rate limit
                        continue
                    else:
                        logging.error(f"API Error for {endpoint}: {error_msg}")
                        return None
                    
                return data['result']
                
            except requests.RequestException as e:
                logging.warning(f"Request attempt {attempt + 1} failed for {endpoint}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                continue
            except json.JSONDecodeError as e:
                logging.error(f"JSON decode error for {endpoint}: {e}")
                return None
            except Exception as e:
                logging.error(f"Unexpected error for {endpoint}: {e}")
                return None
        
        logging.error(f"All retry attempts failed for {endpoint}")
        return None
    
    def get_all_problems(self) -> List[Dict]:
        """Fetch all problems using problemset.problems API"""
        logging.info("Fetching problems from Codeforces API...")
        
        params = {}
        if self.config.tags_filter:
            params['tags'] = ';'.join(self.config.tags_filter)
        
        result = self.make_api_request('problemset.problems', params)
        if not result:
            logging.error("Failed to fetch problems from API")
            return []
        
        problems = result.get('problems', [])
        problem_statistics = result.get('problemStatistics', [])
        
        # Create a mapping of problem statistics
        stats_map = {}
        for stat in problem_statistics:
            contest_id = stat.get('contestId')
            index = stat.get('index', '')
            if contest_id is not None and index:
                key = f"{contest_id}_{index}"
                stats_map[key] = stat
        
        # Filter problems based on criteria and add statistics
        filtered_problems = []
        for problem in problems:
            if self.should_include_problem(problem):
                # Add statistics to problem
                contest_id = problem.get('contestId')
                index = problem.get('index', '')
                if contest_id is not None and index:
                    key = f"{contest_id}_{index}"
                    if key in stats_map:
                        problem['statistics'] = stats_map[key]
                filtered_problems.append(problem)
        
        logging.info(f"Found {len(problems)} total problems, {len(filtered_problems)} match filters")
        self.stats['total_problems'] = len(filtered_problems)
        return filtered_problems
    
    def should_include_problem(self, problem: Dict) -> bool:
        """Check if problem meets filtering criteria"""
        # Rating filter
        if self.config.min_rating is not None or self.config.max_rating is not None:
            rating = problem.get('rating')
            if rating is None:
                # Include unrated problems only if no rating filter is applied
                return self.config.min_rating is None and self.config.max_rating is None
            
            if self.config.min_rating is not None and rating < self.config.min_rating:
                return False
            if self.config.max_rating is not None and rating > self.config.max_rating:
                return False
        
        # Contest type filter
        if self.config.contest_types:
            problem_type = problem.get('type', 'PROGRAMMING')
            if problem_type not in self.config.contest_types:
                return False
        
        # Skip gym problems if not included
        if not self.config.include_gym:
            contest_id = problem.get('contestId')
            if contest_id is not None and contest_id >= 100000:  # Gym contests typically have ID >= 100000
                return False
        
        return True
    
    def clean_filename(self, text: str) -> str:
        """Clean text to create valid filename"""
        if not text:
            return "unknown"
        
        # Remove HTML tags and special characters
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'[<>:"/\\|?*]', '_', text)
        text = re.sub(r'[^\w\s\-_.]', '', text)
        text = re.sub(r'\s+', '_', text)
        text = text.strip('_. ')
        
        # Limit filename length
        return text[:60] if text else "unknown"
    
    def create_problem_json(self, problem: Dict) -> Dict:
        """Create JSON structure for a problem"""
        contest_id = problem.get('contestId')
        index = problem.get('index', '')
        name = problem.get('name', 'Unknown Problem')
        problem_type = problem.get('type', 'PROGRAMMING')
        rating = problem.get('rating')
        tags = problem.get('tags', [])
        points = problem.get('points')
        
        # Create problem statement link
        problem_link = None
        if contest_id is not None and index:
            if contest_id >= 100000:  # Gym problem
                problem_link = f"https://codeforces.com/gym/{contest_id}/problem/{index}"
            else:  # Regular problem
                problem_link = f"https://codeforces.com/problemset/problem/{contest_id}/{index}"
        
        # Add statistics if available
        statistics = problem.get('statistics', {})
        solved_count = statistics.get('solvedCount')
        
        problem_data = {
            "contest_id": contest_id,
            "problem_index": index,
            "name": name,
            "type": problem_type,
            "rating": rating,
            "tags": tags,
            "problem_statement_link": problem_link,
            "scraped_at": time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
        }
        
        # Add optional fields if they exist
        if points is not None:
            problem_data["points"] = points
        if solved_count is not None:
            problem_data["solved_count"] = solved_count
        
        return problem_data
    
    def save_problem(self, problem: Dict) -> bool:
        """Save individual problem to JSON file in appropriate rating bucket"""
        try:
            contest_id = problem.get('contestId', 0)
            index = problem.get('index', 'X')
            name = problem.get('name', 'Unknown Problem')
            rating = problem.get('rating')
            
            # Determine rating bucket
            bucket_name = self._get_rating_bucket(rating)
            bucket_path = self.stats['bucket_stats'][bucket_name]['path']
            
            # Create filename
            clean_name = self.clean_filename(name)
            if contest_id is not None:
                filename = f"{contest_id}_{index}_{clean_name}.json"
            else:
                filename = f"unknown_{index}_{clean_name}.json"
            filepath = os.path.join(bucket_path, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                with self.lock:
                    self.stats['skipped'] += 1
                    self.stats['bucket_stats'][bucket_name]['count'] += 1
                return True
            
            # Create JSON content
            problem_json = self.create_problem_json(problem)
            
            # Write to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(problem_json, f, ensure_ascii=False, indent=2)
            
            with self.lock:
                self.stats['successful'] += 1
                self.stats['bucket_stats'][bucket_name]['count'] += 1
                self.processed_count += 1
                if self.processed_count % 50 == 0:
                    logging.info(f"Processed {self.processed_count}/{self.stats['total_problems']} problems...")
            
            return True
            
        except Exception as e:
            logging.error(f"Error saving problem {problem.get('name', 'Unknown')}: {e}")
            with self.lock:
                self.stats['failed'] += 1
            return False
    
    def save_summary_json(self):
        """Save a summary JSON file with all problems organized by buckets"""
        try:
            summary = {
                "scraping_info": {
                    "scraped_at": time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime()),
                    "total_problems": self.stats['total_problems'],
                    "successful": self.stats['successful'],
                    "failed": self.stats['failed'],
                    "skipped": self.stats['skipped'],
                    "configuration": {
                        "tags_filter": self.config.tags_filter,
                        "min_rating": self.config.min_rating,
                        "max_rating": self.config.max_rating,
                        "language": self.config.language,
                        "bucket_size": self.config.bucket_size,
                        "include_gym": self.config.include_gym
                    }
                },
                "rating_buckets": {},
                "bucket_statistics": {}
            }
            
            # Add bucket information
            for bucket_name, (min_rating, max_rating) in self.rating_buckets.items():
                summary["rating_buckets"][bucket_name] = {
                    "min_rating": min_rating,
                    "max_rating": max_rating,
                    "problems_count": self.stats['bucket_stats'][bucket_name]['count'],
                    "directory": bucket_name
                }
                
                summary["bucket_statistics"][bucket_name] = self.stats['bucket_stats'][bucket_name]['count']
            
            # Save summary file
            summary_path = os.path.join(self.config.output_dir, "scraping_summary.json")
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            
            logging.info(f"Summary saved to {summary_path}")
            
        except Exception as e:
            logging.error(f"Error saving summary: {e}")
    
    def scrape_all_problems(self):
        """Main method to scrape all problems using conservative threading"""
        start_time = time.time()
        
        logging.info("Starting Enhanced Codeforces Scraper (JSON Output)")
        logging.info(f"Output directory: {self.config.output_dir}")
        logging.info(f"Max workers: {self.config.max_workers}")
        logging.info(f"Tag filters: {self.config.tags_filter or 'None'}")
        logging.info(f"Rating range: {self.config.min_rating or 'Any'} - {self.config.max_rating or 'Any'}")
        logging.info(f"Rating buckets: {list(self.rating_buckets.keys())}")
        
        # Get all problems
        problems = self.get_all_problems()
        if not problems:
            logging.error("No problems found. Exiting.")
            return
        
        logging.info(f"Processing {len(problems)} problems with {self.config.max_workers} workers...")
        
        # Process problems with conservative ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_problem = {
                executor.submit(self.save_problem, problem): problem 
                for problem in problems
            }
            
            # Process completed tasks
            for future in as_completed(future_to_problem):
                problem = future_to_problem[future]
                try:
                    future.result()
                except Exception as e:
                    with self.lock:
                        self.stats['failed'] += 1
                    logging.error(f"Task failed for problem {problem.get('name', 'Unknown')}: {e}")
        
        # Save summary
        self.save_summary_json()
        
        # Final summary
        self.print_summary(start_time)
    
    def print_summary(self, start_time: float):
        """Print comprehensive scraping summary"""
        end_time = time.time()
        duration = end_time - start_time
        
        total_processed = self.stats['successful'] + self.stats['failed'] + self.stats['skipped']
        
        print("\n" + "="*60)
        print("CODEFORCES SCRAPING COMPLETED (JSON FORMAT)")
        print("="*60)
        print(f"\nSTATISTICS:")
        print(f"  Total Problems Found: {self.stats['total_problems']}")
        print(f"  Successfully Saved: {self.stats['successful']}")
        print(f"  Skipped (Already Exists): {self.stats['skipped']}")
        print(f"  Failed: {self.stats['failed']}")
        print(f"  Total Processed: {total_processed}")
        
        print(f"\nRATING BUCKET DISTRIBUTION:")
        for bucket_name in sorted(self.rating_buckets.keys()):
            if bucket_name == 'unrated':
                continue
            count = self.stats['bucket_stats'][bucket_name]['count']
            min_r, max_r = self.rating_buckets[bucket_name]
            if bucket_name == 'unrated':
                range_str = "Unrated"
            elif max_r is None:
                range_str = f"{min_r}+"
            else:
                range_str = f"{min_r}-{max_r}"
            print(f"  {bucket_name:15} ({range_str:10}): {count:4} problems")
        
        # Show unrated last
        unrated_count = self.stats['bucket_stats']['unrated']['count']
        print(f"  {'unrated':15} {'(Unrated)':12}: {unrated_count:4} problems")
        
        print(f"\nPERFORMANCE:")
        print(f"  Duration: {duration:.2f} seconds")
        if total_processed > 0:
            print(f"  Average per Problem: {duration/total_processed:.3f} seconds")
            print(f"  Problems per Second: {total_processed/duration:.2f}")
        
        print(f"\nOUTPUT:")
        print(f"  Directory: {os.path.abspath(self.config.output_dir)}")
        print(f"  JSON Files Created: {self.stats['successful']}")
        print(f"  Summary File: scraping_summary.json")
        
        print(f"\nCONFIGURATION:")
        print(f"  Max Workers: {self.config.max_workers}")
        print(f"  Tag Filters: {self.config.tags_filter or 'None'}")
        print(f"  Rating Range: {self.config.min_rating or 'Any'} - {self.config.max_rating or 'Any'}")
        print(f"  Language: {self.config.language}")
        print(f"  Include Gym: {self.config.include_gym}")
        print("="*60)

def create_config_from_args() -> ScrapingConfig:
    """Create configuration from command line arguments"""
    parser = argparse.ArgumentParser(description='Enhanced Codeforces Problem Scraper - JSON Output')
    
    parser.add_argument('--output-dir', default='codeforces_problems_json',
                       help='Output directory for problems (default: codeforces_problems_json)')
    parser.add_argument('--workers', type=int, default=10,
                       help='Number of worker threads (default: 10, max recommended: 15)')
    parser.add_argument('--tags', nargs='+',
                       help='Filter by tags (e.g., --tags implementation math)')
    parser.add_argument('--min-rating', type=int,
                       help='Minimum difficulty rating')
    parser.add_argument('--max-rating', type=int,
                       help='Maximum difficulty rating')
    parser.add_argument('--language', choices=['en', 'ru'], default='en',
                       help='Language for problem content (default: en)')
    parser.add_argument('--include-gym', action='store_true',
                       help='Include gym problems')
    parser.add_argument('--bucket-size', type=int, default=400,
                       help='Rating bucket size (default: 400)')
    
    args = parser.parse_args()
    
    # Validate workers count
    if args.workers > 15:
        print("Warning: Using more than 15 workers may cause rate limiting issues.")
        args.workers = 15
    
    return ScrapingConfig(
        output_dir=args.output_dir,
        max_workers=args.workers,
        tags_filter=args.tags,
        min_rating=args.min_rating,
        max_rating=args.max_rating,
        language=args.language,
        include_gym=args.include_gym,
        bucket_size=args.bucket_size
    )

def main():
    """Main function with enhanced configuration options"""
    
    print("Enhanced Codeforces Problem Scraper - JSON Output with Rating Buckets")
    print("=" * 70)
    
    try:
        # Create configuration
        if len(sys.argv) > 1:
            config = create_config_from_args()
        else:
            # Default config when run without arguments
            config = ScrapingConfig(output_dir='codeforces_problems_json')
        
        # Display configuration
        print(f"Output Directory: {config.output_dir}")
        print(f"Workers: {config.max_workers}")
        print(f"Tags Filter: {config.tags_filter or 'All tags'}")
        print(f"Rating Range: {config.min_rating or 'Any'} - {config.max_rating or 'Any'}")
        print(f"Language: {config.language}")
        print(f"Include Gym: {config.include_gym}")
        print(f"Output Format: JSON")
        print(f"Rating Buckets: Enabled")
        print("-" * 70)
        
        scraper = EnhancedCodeforcesScraper(config)
        scraper.scrape_all_problems()
        
        print(f"\nScraping completed! Check '{config.output_dir}' folder for all problems.")
        print("Problems are organized in rating buckets (folders)")
        print("Check 'scraping_summary.json' for detailed statistics")
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        logging.error(f"Main execution error: {e}")

if __name__ == "__main__":
    main()