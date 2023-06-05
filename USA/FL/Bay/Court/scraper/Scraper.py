import sys
from pathlib import Path
import time
import os
import uuid
from absl import app
from absl import flags
# Try out ABSL's logging, see if it sucks.
from absl import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException, NoSuchElementException, TimeoutException

from requests.exceptions import HTTPError, Timeout
from requests_toolbelt.utils import dump
import requests

p = Path(__file__).resolve().parents[5]
sys.path.insert(1, str(p))
#from common.captcha.benchmark.BenchmarkAdditionSolver import CaptchaSolver
from common.pii import Pii
from common.record import Charge, ChargeBuilder
import utils.ScraperUtils as ScraperUtils
from utils.ScraperUtils import BenchmarkRecordBuilder

# captcha solver
from twocaptcha import TwoCaptcha
from twocaptcha.api import ApiException

# use atexit to ensure that we clean up Selenium
import atexit

import csv

FLAGS = flags.FLAGS
flags.DEFINE_string('portal_base', 'https://court.baycoclerk.com/BenchmarkWeb2/', 'Base of the portal to scrape.')
flags.DEFINE_string('state', 'FL', 'State code we are scraping.', short_name='s')
flags.DEFINE_string('county', 'Bay', 'County we are scraping.', short_name='c')

flags.DEFINE_integer('start_year', 2000, 'Year at which to start scraping.', short_name='y')
flags.DEFINE_integer('end_year', datetime.now().year, 'Year at which to end scraping', short_name='e')

flags.DEFINE_bool('solve_captchas', True, 'Whether to solve captchas.')
flags.DEFINE_enum('save_attachments', 'none', ['none', 'filing', 'all'], 'Which attachments to save.', short_name='a')
flags.DEFINE_string('output', 'bay-county-scraped.csv', 'Relative filename for our CSV', short_name='o')
flags.DEFINE_string('output_requested_dockets', 'bay-county-requested-dockets.csv', 'Relative filename for dockets we have put in a request for')

flags.DEFINE_integer('missing_thresh', 30, 'Number of consecutive missing records after which we move to the next year', short_name='t')
flags.DEFINE_integer('connect_thresh', 10, 'Number of failed connection attempts allowed before giving up')

flags.DEFINE_string('captcha_api_key', 'fake key', '2Captcha API Key')

# TODO(mcsaucy): move everything over to absl.logging so we get this for free
flags.DEFINE_bool('verbose', False, 'Whether to be noisy.')

output_attachments = os.path.join(os.getcwd(), 'attachments')

ffx_profile = webdriver.FirefoxOptions()

# Automatically dismiss unexpected alerts.
ffx_profile.set_capability('unexpectedAlertBehaviour', 'dismiss')
#ffx_profile.add_argument('-headless')

def cleanup_selenium():
    logging.info("Script exiting, make sure that Selenium driver is killed..")
    driver.quit()

if os.getenv('DOCKERIZED') == 'true':
    # If running through docker-compose, use the standalone firefox container. See: docker-compose.yml#firefox
    driver = webdriver.Remote(
       command_executor='http://firefox:4444/wd/hub',
       desired_capabilities=ffx_profile.to_capabilities())
    
    atexit.register(cleanup_selenium)
else:
    driver = webdriver.Firefox(options=ffx_profile)
    atexit.register(cleanup_selenium)

def main(argv):
    del argv

    # Initialize log level
    if FLAGS.verbose:
        logging.set_verbosity(logging.DEBUG)
    else:
        logging.set_verbosity(logging.INFO)

    # Initialize 2captcha
    if FLAGS.solve_captchas:
        global recaptchasolver
        recaptchasolver = TwoCaptcha(FLAGS.captcha_api_key)

    begin_scrape()


def begin_scrape():
    """
    Starts the scraping process. Continues from the last scraped record if the scraper was stopped before.
    :return:
    """
    global driver

    # open output csv file where we store a list of dockets that have been requested
    global requested_dockets_writer
    fieldnames = ['requested_epoch','case_number','docket_number','docket_text']
    requested_dockets_csv = open(FLAGS.output_requested_dockets, 'a', newline='')
    requested_dockets_writer = csv.DictWriter(requested_dockets_csv, fieldnames=fieldnames)
    # This results in the header being written multiple times.. whatever.
    requested_dockets_writer.writeheader()
    
    # Find the progress of any past scraping runs to continue from then
    try:
        last_case_number = ScraperUtils.get_last_csv_row(FLAGS.output).split(',')[3]
        logging.info("Continuing from last scrape (Case number: {})".format(last_case_number))
        last_year = 2000 + int(str(last_case_number)[:2])  # I know there's faster ways of doing this. It only runs once ;)
        if not last_case_number.isnumeric():
            last_case_number = last_case_number[:-4]
        last_case = int(str(last_case_number)[-6:])
        FLAGS.end_year = last_year
        continuing = True
    except FileNotFoundError:
        # No existing scraping CSV
        continuing = False
        pass

    # Scrape from the most recent year to the oldest.
    for year in range(FLAGS.end_year, FLAGS.start_year, -1):
        if continuing:
            N = last_case + 1
        else:
            N = 1

        logging.info("Scraping year {} from case {}".format(year, N))
        YY = year % 100

        record_missing_count = 0

        # Keep a count of cases we've scraped since resetting cookies.
        # Seems like they ask for a captcha once, allow 5 case lookups, and then ask for a captcha every time.
        # So, do a total of 5 case lookups, and then reset the cookies.
        cases_scraped_since_reset = 0

        # Increment case numbers until the threshold missing cases is met, then advance to the next year.
        while record_missing_count < FLAGS.missing_thresh:
            # Generate the case number to scrape
            case_number = f'{YY:02}' + f'{N:06}'

            logging.info(f"Year {year} case {case_number}: Scrape started.")

            search_result = search_portal(case_number)
            cases_scraped_since_reset += 1

            if search_result:
                record_missing_count = 0
                # if multiple associated cases are found,
                # scrape all of them
                if len(search_result) > 1:
                    for case in search_result:
                        logging.info(f"Year {year} case {case_number}: Scraping additional case {case}..")
                        search_portal(case)
                        # TODO: Do we need to validate that the additional case lookup doesn't return multiple cases?
                        cases_scraped_since_reset += 1
                        scrape_record(case)
                # only a single case, no multiple associated cases found
                else:
                    scrape_record(case_number)

            else:
                logging.info(f"Year {year} case {case_number}: Case not found! Incrementing missing count..")
                record_missing_count += 1

            logging.info(f"Year {year} case {case_number}: Scrape complete.")

            # Flush the requested records csv to make sure we keep the updates.
            requested_dockets_csv.flush()

            # If we've scraped 5 or more cases, delete cookies, and reset counter.
            if cases_scraped_since_reset >= 5:
                logging.info("Resetting cookies to attempt to get multiple cases with a single Captcha again.")
                driver.delete_all_cookies()
                cases_scraped_since_reset = 0

            N += 1

        continuing = False

        logging.info("Scraping for year {} is complete".format(year))

    requested_dockets_csv.close()


def scrape_record(case_number):
    """
    Scrapes a record once the case has been opened.
    :param case_number: The current case's case number.
    """
    # Wait for court summary to load
    for i in range(FLAGS.connect_thresh):
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, 'summaryAccordion')))
        except TimeoutException:
            if i == FLAGS.connect_thresh - 1:
                raise RuntimeError('Summary details did not load for case {}.'.format(case_number))
            else:
                driver.refresh()

    # Get relevant page content
    summary_table_col1 = driver.find_elements(by=By.XPATH, value='//*[@id="summaryAccordionCollapse"]/table/tbody/tr/td[1]/dl/dd')
    summary_table_col2 = driver.find_elements(by=By.XPATH, value='//*[@id="summaryAccordionCollapse"]/table/tbody/tr/td[2]/dl/dd')
    summary_table_col3 = driver.find_elements(by=By.XPATH, value='//*[@id="summaryAccordionCollapse"]/table/tbody/tr/td[3]/dl/dd')

    # Wait for court dockets to load
    for i in range(FLAGS.connect_thresh):
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, 'gridDocketsView')))
        except TimeoutException:
            if i == FLAGS.connect_thresh - 1:
                raise RuntimeError('Dockets did not load for case {}.'.format(case_number))
            else:
                driver.refresh()

    charges_table = driver.find_elements(by=By.XPATH, value='//*[@id="gridCharges"]/tbody/tr')
    docket_public_defender = driver.find_elements(by=By.XPATH, value="//*[contains(text(), 'COURT APPOINTED ATTORNEY') and contains(text(), 'ASSIGNED')]")
    docket_attorney = driver.find_elements(by=By.XPATH, value="//*[contains(text(), 'DEFENSE') and contains(text(), 'ASSIGNED')]")
    docket_pleas = driver.find_elements(by=By.XPATH, value="//*[contains(text(), 'PLEA OF')]")
    docket_attachments = driver.find_elements(by=By.CLASS_NAME, value='casedocketimage')
    docket_attachments_to_request = driver.find_elements(by=By.CLASS_NAME, value='popmodal')

    # Process dockets that aren't available but are requestable..
    # TODO: Store the fact that we've requested these dockets, so we know to come back for 'em later.
    # Disable this for now.. older cases have a ton and it takes for-freakin-ever.
    if FLAGS.save_attachments and False:
        # Some dockets aren't available, but are requestable.
        # For those dockets, we need to send a POST to:
        # https://court.baycoclerk.com/BenchmarkWeb2/CaseDocket.aspx/Request
        # with "caseDocketID=<x>&email="

        for request_link in docket_attachments_to_request:
            attachment_text = request_link.find_element(by=By.XPATH, value='./../../td[3]').text.strip()
            case_docket_id = request_link.get_attribute('casedocketid')
            
            # Copy Selenium's user agent and headers to requests - this is from the save_attached_pdf function.
            user_agent = driver.execute_script('return navigator.userAgent;')
            session = requests.Session()
            host = FLAGS.portal_base.split('/')[2]
            session.headers.update({'User-Agent': user_agent, 'Host': host, 'Connection': 'keep-alive', 'Accept-Language': 'en-US,en;q=0.5', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': 'text/css,*/*;q=0.1'})
            portal_cookies = driver.get_cookies()
            cookie_header = ''
            for cookie in portal_cookies:
                cookie_header += '{}={}; '.format(cookie['name'], cookie['value'])
            cookie_header = cookie_header[:-2]  # Remove last deliminator '; '

            try:
                docketrequestlink = f'{FLAGS.portal_base}/BenchmarkWeb2/CaseDocket.aspx/Request'
                raw_data=f"caseDocketID={case_docket_id}&email="
                result = session.post(url=docketrequestlink, headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Referer': driver.current_url,
                    'Upgrade-Insecure-Requests': '1',
                    'Cookie': cookie_header
                }, data=raw_data)
                result.raise_for_status()
                logging.info(f"Case {case_number}: Requested docket attachment {case_docket_id}-{attachment_text} to be posted.")

                # Write to the CSV
                requested_dockets_writer.writerow(
                    {
                        'requested_epoch': int( time.time() ),
                        'case_number': case_number,
                        'docket_number': case_docket_id,
                        'docket_text': attachment_text
                    })
            except HTTPError as http_err:
                logging.warn(f'HTTP error occurred while requesting docket {case_docket_id}; continuing. Error: {http_err}')
                pass
            except Timeout:
                logging.warn(f'HTTP timeout occurred while requesting docket {case_docket_id}; continuing.')
                pass

    r = BenchmarkRecordBuilder()
    r.id = str(uuid.uuid4())
    r.state = FLAGS.state
    r.county = FLAGS.county
    r.portal_id = case_number
    r.case_num = Pii.String(summary_table_col2[1].text.strip())
    r.agency_report_num = summary_table_col1[4].text.strip()
    r.arrest_date = None  # Can't be found on this portal
    r.filing_date = summary_table_col1[2].text.strip()
    r.offense_date = None  # Can't be found on this portal
    r.division_name = summary_table_col3[3].text.strip()
    r.case_status = summary_table_col3[1].text.strip()

    # Create list of assigned defense attorney(s)
    defense_attorney_text = list(map(lambda x: x.text, docket_attorney))
    r.defense_attorney = ScraperUtils.parse_attorneys(
            defense_attorney_text)
    # Create list of assigned public defenders / appointed attorneys
    public_defender_text = list(map(lambda x: x.text, docket_public_defender))
    r.public_defender = ScraperUtils.parse_attorneys(
            public_defender_text)
    # Get Judge
    r.judge = Pii.String(summary_table_col1[0].text.strip())

    # Download docket attachments.
    # Todo(OscarVanL): This could be parallelized to speed up scraping if save-attachments is set to 'all'.
    if FLAGS.save_attachments:
        for attachment_link in docket_attachments:
            attachment_text = attachment_link.find_element(by=By.XPATH, value='./../../td[3]').text.strip()
            if FLAGS.save_attachments == 'filing':
                if not ('CITATION FILED' in attachment_text or 'CASE FILED' in attachment_text):
                    # Attachment is not a filing, don't download it.
                    continue
            logging.info(f"Case {case_number}: Downloading docket attachment: {case_number}-{attachment_text}.")
            ScraperUtils.save_attached_pdf(driver=driver, directory=output_attachments, name='{}-{}'.format(case_number, attachment_text),
                                           portal_base=FLAGS.portal_base, download_href=attachment_link, logging=logging, timeout=20)

    Charges = {}
    for charge in charges_table:
        charge_builder = ChargeBuilder()
        charge_cols = charge.find_elements(by=By.TAG_NAME, value='td')
        count = int(charge_cols[0].text.strip())
        charge_builder.count = count

        charge_desc = charge_cols[1].text
        charge_builder.description, charge_builder.statute = (
                ScraperUtils.parse_charge_statute(charge_desc))
        charge_builder.level = charge_cols[2].text.strip()
        charge_builder.degree = charge_cols[3].text.strip()
        # plea = charge_cols[4].text.strip() # Plea is not filled out on this portal.
        charge_builder.disposition = charge_cols[5].text.strip()
        charge_builder.disposition_date = charge_cols[6].text.strip()
        Charges[count] = charge_builder.build()
    r.charges = list(Charges.values())

    # Pleas are not in the 'plea' field, but instead in the dockets.
    for plea_element in docket_pleas:
        plea_text = plea_element.text.strip()
        plea = ScraperUtils.parse_plea_type(plea_text)
        plea_date = plea_element.find_element(by=By.XPATH, value='./../td[2]').text.strip()
        plea_number = ScraperUtils.parse_plea_case_numbers(plea_text, list(Charges.keys()))

        # If no case number is specified in the plea, then we assume it applies to all charges in the trial.
        if len(plea_number) == 0:
            for charge in Charges.values():
                charge.plea = plea
                charge.plea_date = plea_date
        else:
            # Apply plea to relevant charge count(s).
            for count in plea_number:
                Charges[count].plea = plea
                Charges[count].plea_date = plea_date

    r.arresting_officer = None  # Can't be found on this portal
    r.arresting_officer_badge_number = None  # Can't be found on this portal

    profile_link = driver.find_element(by=By.XPATH, value="//table[@id='gridParties']/tbody/tr/*[contains(text(), 'DEFENDANT')]/../td[2]/div/a").get_attribute(
       'href')
    # profile_link = driver.find_element(by=By.XPATH, value='//*[@id="gridParties"]/tbody/tr[1]/td[2]/div[1]/a').get_attribute(
    #     'href')
    load_page(profile_link, 'Party Details:', FLAGS.verbose)

    r.suffix = None
    r.dob = None  # This portal has DOB as N/A for every defendent
    r.race = driver.find_element(by=By.XPATH, value='//*[@id="fd-table-2"]/tbody/tr[2]/td[2]/table[2]/tbody/tr/td[2]/table/tbody/tr[7]/td[2]').text.strip()
    r.sex = driver.find_element(by=By.XPATH, value='//*[@id="mainTableContent"]/tbody/tr/td/table/tbody/tr[2]/td[2]/table[2]/tbody/tr/td[2]/table/tbody/tr[6]/td[2]').text.strip()

    # Navigate to party profile
    full_name = driver.find_element(by=By.XPATH, value='//*[@id="mainTableContent"]/tbody/tr/td/table/tbody/tr[2]/td[2]/table[2]/tbody/tr/td[2]/table/tbody/tr[1]/td[2]').text.strip()
    r.middle_name = None
    r.last_name = None
    if ',' in full_name:
        r.first_name, r.middle_name, r.last_name = ScraperUtils.parse_name(full_name)
    else:
        # If there's no comma, it's a corporation name.
        r.first_name = Pii.String(full_name)
    r.party_id = driver.find_element(by=By.XPATH, value='//*[@id="mainTableContent"]/tbody/tr/td/table/tbody/tr[2]/td[2]/table[2]/tbody/tr/td[2]/table/tbody/tr[8]/td[2]').text.strip()  # PartyID is a field within the portal system to uniquely identify defendants

    record = r.build()
    ScraperUtils.write_csv(FLAGS.output, record, FLAGS.verbose)

def search_portal(case_number):
    """
    Performs a search of the portal from its home page, including selecting the case number input, solving the captcha
    and pressing Search. Also handles the captcha being solved incorrectly
    :param case_number: Case to search
    :return: A set of case number(s).
    """
    # Load portal search page
    search_page=f"{FLAGS.portal_base}/Home.aspx/Search"
    load_page(search_page, 'Search', FLAGS.verbose)
    # Give some time for the captcha to load, as it does not load instantly.
    time.sleep(0.8)

    # Select Case Number textbox and enter case number
    select_case_input()
    case_input = driver.find_element(by=By.ID, value='caseNumber')
    case_input.click()
    case_input.send_keys(case_number)

    # Solve captcha if it is required
    try:
        # Get Captcha. This is kinda nasty, but if there's no Captcha, then
        # this will throw (which is a good thing in this case) and we can
        # move on with processing.
        recaptchav2_sitekey = driver.find_element(by=By.XPATH, value='//*/div[@class="g-recaptcha"]').get_attribute("data-sitekey")
        
        logging.info(f"Case {case_number}: Captcha encoutered; solving via service..")

        if FLAGS.solve_captchas:
            logging.debug(f"Solving captcha with data-sitekey of: {recaptchav2_sitekey}")

            # TODO: Retries
            try:
                result = recaptchasolver.recaptcha(sitekey=recaptchav2_sitekey, url=search_page)
            except ApiException as e:
                logging.error(f"TwoCaptcha API Exception; exiting. Failure: {e}")
                exit(1)
            except Exception as e:
                logging.error(f"TwoCaptcha other exception; exiting. Failure: {e}")
                exit(1)

            logging.debug(f"Captcha solver results: {str(result)}")

            # Fill in the field
            driver.execute_script('document.getElementById("g-recaptcha-response").innerHTML = "{}";'.format(result["code"]))

            logging.info(f"Case {case_number}: Captcha solved; submitting result.")

            # Do search
            search_button = driver.find_element(by=By.ID, value='searchButton')
            search_button.click()
        else:
            logging.info(f"Captcha encountered trying to view case ID {case_number}.")
            logging.info("Please solve the captcha and click the search button to proceed.")
            while True:
                try:
                    WebDriverWait(driver, 6 * 60 * 60).until(
                        lambda x: case_number in driver.title )
                    
                    logging.info(f"Case {case_number}: Captcha solved manually.")

                    logging.info("continuing...")
                    break
                except TimeoutException:
                    logging.info("still waiting for user to solve the captcha...")

    except NoSuchElementException:
        # No captcha on the page, continue.
        # Do search
        search_button = driver.find_element(by=By.ID, value='searchButton')
        search_button.click()

    # If the title stays as 'Search': Captcha solving failed
    # If the title contains the case number or 'Search Results': Captcha solving succeeded
    # If a timeout occurs, retry 'connect_thresh' times.
    for i in range(FLAGS.connect_thresh):
        try:
            # Wait for page to load
            WebDriverWait(driver, 5).until(
                lambda x: 'Search' in driver.title or case_number in driver.title or 'Search Results:' in driver.title)
            # Page loaded
            if driver.title == 'Search':
                # Clicking search did not change the page. This could be because of a failed captcha attempt.
                try:
                    # Check if 'Invalid Captcha' dialog is showing
                    # Confirmed that this is still what is returned for a bad captcha!
                    driver.find_element(by=By.XPATH, value='//div[@class="alert alert-error"]')
                    logging.info("Captcha was solved incorrectly")
                except NoSuchElementException:
                    pass
                # Clear cookies so a new captcha is presented upon refresh
                driver.delete_all_cookies()
                # Try solving the captcha again.
                search_portal(case_number)
            elif 'Search Results: CaseNumber:' in driver.title:
                # Captcha solved correctly

                # Figure out the number of cases returned
                case_count = ScraperUtils.get_search_case_count(driver, FLAGS.county)
                # Case number search found multiple cases.
                if case_count > 1:
                    return ScraperUtils.get_associated_cases(driver)
                # Case number search found no cases
                else:
                    return set()
            elif case_number in driver.title:
                # Captcha solved correctly

                # Case number search did find a single court case.
                return {case_number}
        except TimeoutException:
            if i == FLAGS.connect_thresh - 1:
                raise RuntimeError('Case page could not be loaded after {} attempts, or unexpected page title: {}'.format(FLAGS.connect_thresh, driver.title))
            else:
                search_portal(case_number)


def select_case_input():
    """
    Selects the Case Number input on the Case Search window.
    """
    # Wait for case selector to load
    for i in range(FLAGS.connect_thresh):
        try:
            WebDriverWait(driver, 5).until(EC.text_to_be_present_in_element((By.ID, 'title'), 'Case Search'))
        except TimeoutException:
            if i == FLAGS.connect_thresh - 1:
                raise RuntimeError('Portal homepage could not be loaded')
            else:
                load_page(f"{FLAGS.portal_base}/Home.aspx/Search", 'Search', FLAGS.verbose)

    case_selector = driver.find_element(by=By.XPATH, value='//*/input[@searchtype="CaseNumber"]')
    case_selector.click()
    try:
        case_input = driver.find_element(by=By.ID, value='caseNumber')
        case_input.click()
    except ElementNotInteractableException:
        # Sometimes the caseNumber box does not appear, this is resolved by clicking to another radio button and back.
        name_selector = driver.find_element(by=By.XPATH, value='//*/input[@searchtype="Name"]')
        name_selector.cick()
        case_selector.click()
        case_input = driver.find_element(by=By.ID, value='caseNumber')
        case_input.click()

    return case_input


def load_page(url, expectedTitle, verbose=False):
    """
    Loads a page, but tolerates intermittent connection failures up to 'connect-thresh' times.
    :param url: URL to load
    :param expectedTitle: Part of expected page title if page loads successfully. Either str or list[str].
    """
    if verbose:
        logging.info('Loading page:', url)
    driver.get(url)
    for i in range(FLAGS.connect_thresh):
        try:
            if isinstance(expectedTitle, str):
                WebDriverWait(driver, 5).until(EC.title_contains(expectedTitle))
                return
            elif isinstance(expectedTitle, list):
                WebDriverWait(driver, 5).until(any(x in driver.title for x in expectedTitle))
                return
            else:
                raise ValueError('Unexpected type passed to load_page. Allowed types are str, list[str]')
        except TimeoutException:
            if i == FLAGS.connect_thresh - 1:
                raise RuntimeError('Page {} could not be loaded after {} attempts. Check connction.'.format(url, FLAGS.connect_thresh))
            else:
                if verbose:
                    logging.info('Retrying page (attempt {}/{}): {}'.format(i+1, FLAGS.connect_thresh, url))
                driver.get(url)

    logging.info('Page {} could not be loaded after {} attempts. Check connection.'.format(url, FLAGS.connect_thresh),
          file=sys.stderr)


if __name__ == '__main__':
    if not os.path.exists(output_attachments):
        os.makedirs(output_attachments)   

    app.run(main)
