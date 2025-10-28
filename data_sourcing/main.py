import os
import re
import sys
from enum import Enum
from time import sleep, time
from urllib.parse import quote, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession

# sources all data required for the project
# step 1. get all rankings from the ufc website
# step 2. get all fighter urls from the tapology search page
# step 3. get all bout data from the fighters main page
# all jsons are stored flat for ease of use later on
# scraping is done with requests + beautifulsoup
# final output is for use in a flutter app


# All main data structures will use pandas DataFrames instead of lists/dicts.
# Example: fighter_data = pd.DataFrame([...])
# When loading/saving, use pd.read_json and df.to_json.
# it'll be a flat json file with all bout data
# all outputs should be stored in the data/ folder

# global variables
ufc_rankings_url = "https://www.ufc.com/rankings"
tapology_url = "https://www.tapology.com"
data_folder = "data/"


# Output files in assets/assets/
ufc_ranking_file = "assets/assets/ufc_rankings.json"
bout_data_file = "assets/assets/bout_data.json"


class FightResult(Enum):
    WIN = 'W'
    LOSS = 'L'
    DRAW = 'D'
    NO_CONTEST = 'NC'
    SCHEDULED = 'S'
    CANCELLED = 'C'


bout_data_columns = [
    'principle_fighter',    'opponent_fighter', 'result', 'fight_date'
]

# Utility functions


def retry_with_backoff(func, max_retries: int = 5, initial_delay: float = 1.0):
    """
    Retry a function with exponential backoff on network errors.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds (doubles each retry)

    Returns:
        The result of the function

    Raises:
        Exception: If all retries fail
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries):
        try:
            return func()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:
            last_exception = e
            attempt_num = attempt + 1

            if attempt_num >= max_retries:
                print(f"    ✗ Failed after {max_retries} attempts: {e}")
                raise

            print(
                f"    ⚠ Network error (attempt {attempt_num}/{max_retries}): {type(e).__name__}")
            print(f"      Retrying in {delay:.1f}s...")
            sleep(delay)
            delay *= 2  # Exponential backoff

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise Exception("Retry function failed unexpectedly")


# Scraping Functions
def scrape_ufc_rankings() -> pd.DataFrame:

    session = HTMLSession()
    response = session.get(ufc_rankings_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all tables on the page
    tables = soup.find_all('table')

    print(
        f"Found {len(tables)} tables on the UFC rankings page.")

    # parse all results into a dataframe
    fighter_data = []

    for table_idx, table in enumerate(tables):

        print(f"Parsing table {table_idx + 1}/{len(tables)}")

        # print(table.prettify()[:500])  # print first 500 chars of the table

        #     <h4>
        #  Men's Pound-for-Pound
        division_header = table.find('h4')
        if division_header is None:
            # print(table)
            raise ValueError("Could not find division header for table.")

        division_name = division_header.text.strip(
        ) if division_header else "Unknown Division"

        # skip pound for pound for now
        if division_name.startswith("Men's Pound-for-Pound") or division_name.startswith("Women's Pound-for-Pound"):
            print("Skipping Pound-for-Pound rankings for now.")
            continue

        # <div class="rankings--athlete--champion clearfix"> if for the champion
        champion_div = table.find('h5')
        if champion_div is None:
            # print(table)
            raise ValueError("Could not find champion div for table.")
        else:
            champion_name = champion_div.text.strip()

        ranking_record = {
            'division': division_name,
            'fighter_name': champion_name,
            'rank': 0,
            'movement': 0,
        }

        fighter_data.append(ranking_record)

        # all the other fighters are in tr tags
        fighter_rows = table.find_all('tr')

        for fighter_row in fighter_rows:

            # print(fighter_row)

            # fighter name is in td with class 'views-field-title'
            fighter_div = fighter_row.find(
                'td', class_="views-field views-field-title")
            if fighter_div is None:
                raise ValueError("Could not find fighter div in row.")
            else:
                fighter_name = fighter_div.text.strip()

            rank_string = fighter_row.find(
                'td', class_="views-field views-field-weight-class-rank")
            if rank_string is None:
                raise ValueError("Could not find rank string in row.")
            else:
                rank = int(rank_string.text.strip().replace('#', ''))

            movement_div = fighter_row.find(
                'td', class_='views-field views-field-weight-class-rank-change')

            # movement string is of the form Rank increased by X or Rank decreased by X. parse to extract X and the direction, as an int
            if movement_div:
                movement_match = re.search(
                    r'Rank (increased|decreased) by (\d+)', movement_div.text.strip())
                if movement_match:
                    direction = 1 if movement_match.group(
                        1) == 'increased' else -1
                    movement = direction * int(movement_match.group(2))
                else:
                    movement = 0
            else:
                movement = 0

            ranking_record = {
                'division': division_header.text.strip() if division_header else 'Unknown',
                'fighter_name': fighter_name,
                'rank': rank,
                'movement': movement,
            }

            # print(ranking_record)

            fighter_data.append(ranking_record)

    return pd.DataFrame(fighter_data)


def scrape_tapology_fighter_urls(fighter_data: pd.DataFrame) -> pd.DataFrame:

    url_df = get_url_data(fighter_data)

    # add url data to fighter_data
    fighter_data['fighter_url'] = fighter_data['fighter_name'].map(
        url_df.set_index('fighter_name')['fighter_url'])
    return fighter_data


def get_url_data(fighter_data: pd.DataFrame) -> pd.DataFrame:

    # load cached fighter urls from data/fighter_urls_cache.json
    if os.path.exists('data/fighter_urls_cache.json'):
        cached_fighter_url_df = pd.read_json('data/fighter_urls_cache.json')
    else:
        cached_fighter_url_df = pd.DataFrame(
            columns=['fighter_name', 'fighter_url'])

    url_data = []

    for i, (index, fighter) in enumerate(fighter_data.iterrows()):

        # these urls dont change over time, so we can refer to a cached version 1st
        # and also scrape fighters not found in the cache

        if fighter['fighter_name'] in cached_fighter_url_df['fighter_name'].values:
            cached_url = cached_fighter_url_df.loc[
                cached_fighter_url_df['fighter_name'] == fighter['fighter_name']]['fighter_url'].values[0]
            print(
                f"Using cached URL for fighter {i+1}/{len(fighter_data)}: {fighter['fighter_name']}")

            url_record = {
                'fighter_name': fighter['fighter_name'],
                'fighter_url': cached_url,
            }

            url_data.append(url_record)
            continue
        else:
            print(
                f'Scraping URL {i+1}/{len(fighter_data)}')

            cached_fighter_name = fighter['fighter_name']
            search_url = f'{tapology_url}/search?term={quote(cached_fighter_name, safe="")}&search=Submit+Query&mainSearchFilter=fighters'
            session = HTMLSession()

            # Perform search with retry logic
            def fetch_search_page():
                response = session.get(search_url, timeout=10, )
                response.raise_for_status()
                return response.text

            page_html = retry_with_backoff(
                fetch_search_page, max_retries=15, initial_delay=1.0)
            soup = BeautifulSoup(page_html, 'html.parser')

            # Find the results table - the search results page goes directly to a table
            results_table = soup.find('table', class_='fcLeaderboard')

            if results_table is None or not results_table:
                raise Exception("Could not find search results table")

            # Find the results table - the search results page goes directly to a table
            # results are in a class fcLeaderboard
            results_table = soup.find('table', class_='fcLeaderboard')
            # results_table = soup.find('table', class_='fcLeaderboard')

            if not results_table:
                raise Exception("Could not find search results table")

            # Extract all fighter links from the table rows
            result_links = results_table.find_all('a', href=True)
            result_links = [
                a for a in result_links if '/fightcenter/fighters/' in a['href']]

            if not result_links:
                raise Exception(
                    f"No fighter results found for '{cached_fighter_name}'")

            # Get the first result - should be the best match
            first_result = result_links[0]
            fighter_url = first_result.get('href')

            if not fighter_url:
                raise Exception(
                    "Could not extract URL from first search result")

            # print(f"Found fighter URL for {fighter_name}: {fighter_url}")
            url_record = {
                'fighter_name': fighter['fighter_name'],
                'fighter_url': fighter_url,
            }

            url_data.append(url_record)

            sleep(1)  # be polite to the server

            # print(fighter)

    # save the new file to the cache for the next run
    updated_cache_df = pd.DataFrame(url_data)
    updated_cache_df.to_json('data/fighter_urls_cache.json',
                             orient='records', indent=2, force_ascii=False)

    url_df = pd.DataFrame(url_data)
    return url_df


def scrape_single_fighter_bout_data(fighter_data: pd.Series) -> pd.DataFrame:

    session = HTMLSession()
    full_url = urljoin(tapology_url, fighter_data['fighter_url'])

    def fetch_fighter_page():
        response = session.get(full_url, timeout=10)
        response.raise_for_status()
        return response.text

    page_html = retry_with_backoff(
        fetch_fighter_page, max_retries=15, initial_delay=1.0)

    soup = BeautifulSoup(page_html, 'html.parser')

    # Find the bouts table
    bouts_table = soup.find_all(
        class_='result flex items-center justify-between h-[50px] md:h-[44px]')

    bout_df = pd.DataFrame(columns=bout_data_columns)

    found_opponent_url_count = 0

    for bout in bouts_table:

        # find the link to the opponent's fighter page
        # its href contains /fightcenter/fighters/
        opponent_link = bout.find('a', href=True)
        if opponent_link and '/fightcenter/fighters/' in opponent_link['href']:
            opponent_url = opponent_link['href']
        else:
            # print("Could not find opponent URL for bout.")
            continue

        found_opponent_url_count += 1

        # check if the opponent_url is in the rankings fighter_urls list
        if opponent_url in fighter_df['fighter_url'].values:
            # take the opponent name from the fighter_data for consistency
            opponent_name = fighter_df.loc[
                fighter_df['fighter_url'] == opponent_url]['fighter_name'].values[0]
        else:
            # their not in the rankings, skip
            # print(
            #     f"Opponent {opponent_url} is not in the rankings, skipping bout.")
            continue

        # if its an upcoming bout it'll have a hyperlink with the text Confirmed Upcoming Bout
        upcoming_bout_link = bout.find_all('a', href=True)
        if upcoming_bout_link and any('Confirmed Upcoming Bout' in a.text for a in upcoming_bout_link):
            result = 'S'
        else:
            # W result is stored in <div class="div w-[28px] md:w-[32px] flex shrink-0 items-center justify-center text-white text-opacity-60 text-lg leading-none font-extrabold h-full rounded-l-sm bg-[#29b829] opacity-90">W</div>
            # NCresult is stored in <div class="div w-[28px] md:w-[32px] flex shrink-0 items-center justify-center text-white text-opacity-60 text-lg leading-none font-extrabold h-full rounded-l-sm bg-yellow-400 opacity-90">NC</div>
            result_div = bout.find(
                'div', class_=re.compile(r'flex shrink-0 items-center justify-center text-white'))
            if result_div:
                result = result_div.text.strip()
            else:
                print("Could not find bout result.")
                print(bout.prettify())
                raise ValueError("Could not find bout result.")

        # the fight date is stored in 2 parts
        # <div class="div flex flex-col justify-around items-center rounded border border-[#fcfcfc] md:border-0 px-0.5 pt-1 pb-0.5 md:p-0 basis-full">
        # <span class="text-[13px] md:text-xs text-tap_3 font-bold">2025</span>
        # <span class="text-xs11 text-neutral-600">Oct 25</span>
        # </div>
        date_div = bout.find(
            'div', class_=re.compile(r'flex flex-col justify-around items-center rounded border'))
        if date_div:
            date_spans = date_div.find_all('span')
            if len(date_spans) == 2:
                # convert date to iso date format
                year = date_spans[0].text.strip()
                month_day = date_spans[1].text.strip()
                fight_date = f"{month_day} {year}"
                fight_date = pd.to_datetime(
                    fight_date, format="%b %d %Y").date()

            else:
                print("Could not find fight date spans.")
                raise ValueError("Could not find fight date spans.")
        else:
            print("Could not find fight date div.")
            raise ValueError("Could not find fight date div.")

        single_bout = {
            'principle_fighter': fighter_data['fighter_name'],
            'opponent_fighter': opponent_name,
            'fight_date': fight_date,
            'result': result,
        }

        # print(single_bout)

        bout_df = pd.concat(
            [bout_df, pd.DataFrame([single_bout])], ignore_index=True)

    if found_opponent_url_count == 0:
        print(
            f"Warning: Likely used the wrong url as no ranked opponents found for fighter {fighter_data['fighter_name']} at {full_url}")
        raise ValueError(
            f"No opponent URLs found for fighter {fighter_data['fighter_name']}")

    return bout_df


def scrape_all_fighter_bout_data(fighter_data: pd.DataFrame) -> pd.DataFrame:
    # code to scrape bout data for all fighters in fighter_data

    bout_df = pd.DataFrame(columns=bout_data_columns)

    for i, fighter in enumerate(fighter_data.iterrows()):
        print(
            f"Scraping bout data for fighter {i+1}/{len(fighter_data)}: {fighter[1]['fighter_name']}")
        fighter_url = fighter[1].get('fighter_url')
        if fighter_url:
            bout_df = pd.concat(
                [bout_df, scrape_single_fighter_bout_data(fighter[1])], ignore_index=True)
            sleep(1)  # be polite to the server

    # clean up the data

    # clean up 1
    # if there is a pair of fighters that have a canceled bout (result 'C') and a completed bout
    # keep only the completed bout
    # compare versus the enum FightResult
    cleaned_bout_list = []
    for bout in bout_df.to_dict(orient='records'):
        if bout['result'] == FightResult.CANCELLED.value:
            # check if there is a completed bout for the same fighters
            completed_bout_exists = any(
                b['principle_fighter'] == bout['principle_fighter'] and
                b['opponent_fighter'] == bout['opponent_fighter'] and
                b['result'] in [FightResult.WIN.value, FightResult.LOSS.value,
                                FightResult.NO_CONTEST.value, FightResult.DRAW.value, FightResult.SCHEDULED.value]
                for b in bout_df.to_dict(orient='records'))
            if completed_bout_exists:
                # dont keep the canceled bout
                print(
                    f"Removing canceled bout for {bout['principle_fighter']} vs {bout['opponent_fighter']}")
                continue
        else:
            # keep all other fight result bouts
            cleaned_bout_list.append(bout)

    cleaned_bout_df = pd.DataFrame(cleaned_bout_list)

    # format date to iso format yyyy-mm-dd
    cleaned_bout_df['fight_date'] = pd.to_datetime(
        cleaned_bout_df['fight_date']).dt.strftime('%Y-%m-%d')

    return cleaned_bout_df


def save_data_to_json(df: pd.DataFrame, filename):
    # Ensure output directory exists
    output_dir = os.path.dirname(filename)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    df.to_json(filename, orient="records", indent=2, force_ascii=False)


def data_quality_checks(fighter_data_with_bouts):
    # sense check
    # get a list of every principle fighter, that also appears in the opponent_fighter field
    principle_fighters = set([f['principle_fighter']
                             for f in fighter_data_with_bouts])
    opponent_fighters = set([bout['opponent_fighter']
                            for bout in fighter_data_with_bouts])
    appears_in_both = principle_fighters.intersection(opponent_fighters)

    # the bouts where both fighters are in the appears_in_both set, should have 2 bout entries
    # one where they are the principle fighter, and one where they are the opponent fighter
    for bout in fighter_data_with_bouts:
        principle = bout['principle_fighter']
        opponent = bout['opponent_fighter']
        if principle in appears_in_both and opponent in appears_in_both:
            print(f"Both fighters found in bout: {principle} vs {opponent}")
            # check if the reverse bout exists
            reverse_bout_exists = any(
                b['principle_fighter'] == opponent and b['opponent_fighter'] == principle for b in fighter_data_with_bouts)
            if not reverse_bout_exists:
                raise ValueError(
                    f"Missing reverse bout entry for '{principle}' vs '{opponent}'")


if __name__ == "__main__":

    start_from_step = 1

    # Step 1: Scrape UFC rankings
    try:
        if start_from_step <= 1:
            fighter_df = scrape_ufc_rankings()
            # Save UFC rankings to assets/assets/ufc_rankings.json
            fighter_df.to_json(ufc_ranking_file, orient="records",
                               indent=2, force_ascii=False)
            print("UFC rankings data saved to assets/assets/ufc_rankings.json.")
        else:
            fighter_df = pd.read_json(ufc_ranking_file)
            print("UFC rankings data loaded from file.")
    except Exception as e:
        print(f"Error in Step 1: {e}")
        sys.exit(1)

    # Step 2: Filter for just 2 divisions for testing purposes
    # if start_from_step <= 2:
    #     divisions_to_keep = [
    #         "Light Heavyweight",
    #     ]
    #     fighter_df = fighter_df[fighter_df['division'].isin(divisions_to_keep)]
    #     print(
    #         f"Filtered fighter data to {len(fighter_df)} fighters in divisions: {divisions_to_keep}")

    # Step 3: Scrape Tapology fighter URLs
    try:
        if start_from_step <= 3:
            fighter_df = scrape_tapology_fighter_urls(fighter_df)
            # Do not save fighter_data.json anymore
        else:
            # No loading from fighter_data.json
            pass
    except Exception as e:
        print(f"Error in Step 3: {e}")
        sys.exit(1)

    # Step 4: Scrape bout data for all fighters
    try:
        if start_from_step <= 4:
            bout_df = scrape_all_fighter_bout_data(fighter_df)
            save_data_to_json(bout_df, bout_data_file)
            print("Fighter bout data saved to assets/assets/bout_data.json.")
        else:
            bout_df = pd.read_json(bout_data_file)
            print("Bout data loaded from file.")

        # Re-save bout data with indents (already handled by save_data_to_json)
        save_data_to_json(bout_df, bout_data_file)
        print("Fighter bout data re-saved with indents to assets/assets/bout_data.json.")
    except Exception as e:
        print(f"Error in Step 4: {e}")
        sys.exit(1)

    # # Data Quality Checks
    # if start_from_step <= 5:
    #     data_quality_checks(bout_df)
