import io
import os
import requests
from bs4 import BeautifulSoup
from docx import Document
from datetime import datetime
import pymongo
from deep_translator import GoogleTranslator, exceptions
from docx2pdf import convert
import time
import asyncio
import telegram
import logging
from docx.shared import Inches
import tempfile
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB setup
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')

client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Function to fetch all article URLs from the given pages
def fetch_article_urls(base_url, pages):
    article_urls = []
    for page in range(1, pages + 1):
        url = base_url if page == 1 else f"{base_url}page/{page}/"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        for h1_tag in soup.find_all('h1', id='list'):
            a_tag = h1_tag.find('a')
            if a_tag and a_tag.get('href'):
                article_urls.append(a_tag['href'])
    return article_urls

# Function to translate text to Gujarati with retry mechanism
def translate_to_gujarati(text):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            translator = GoogleTranslator(source='auto', target='gu')
            return translator.translate(text)
        except exceptions.TranslationNotFoundException as e:
            logging.warning(f"Translation not found: {e}")
            return text
        except Exception as e:
            logging.error(f"Error in translation (attempt {attempt + 1}): {e}")
            time.sleep(2)
    return text

# Function to scrape the content and return as a list of paragraphs
async def scrape_and_get_content(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    main_content = soup.find('div', class_='inside_post column content_width')
    if not main_content:
        raise Exception("Main content div not found")
    heading = main_content.find('h1', id='list')
    if not heading:
        raise Exception("Heading not found")
    content_list = []
    heading_text = heading.get_text()
    translated_heading = translate_to_gujarati(heading_text)
    content_list.append({'type': 'heading', 'text': translated_heading})
    content_list.append({'type': 'heading', 'text': heading_text})
    for tag in main_content.find_all(recursive=False):
        if tag.get('class') in [['sharethis-inline-share-buttons', 'st-center', 'st-has-labels', 'st-inline-share-buttons', 'st-animated'], ['prenext']]:
            continue
        text = tag.get_text()
        translated_text = translate_to_gujarati(text)
        if tag.name == 'p':
            content_list.append({'type': 'paragraph', 'text': translated_text})
            content_list.append({'type': 'paragraph', 'text': text})
        elif tag.name == 'h2':
            content_list.append({'type': 'heading_2', 'text': translated_text})
            content_list.append({'type': 'heading_2', 'text': text})
        elif tag.name == 'h4':
            content_list.append({'type': 'heading_4', 'text': translated_text})
            content_list.append({'type': 'heading_4', 'text': text})
        elif tag.name == 'ul':
            for li in tag.find_all('li'):
                li_text = li.get_text()
                translated_li_text = translate_to_gujarati(li_text)
                content_list.append({'type': 'list_item', 'text': f"• {translated_li_text}"})
                content_list.append({'type': 'list_item', 'text': f"• {li_text}"})
    return content_list

def insert_content_between_placeholders(doc, content_list):
    start_placeholder = None
    end_placeholder = None
    
    for i, para in enumerate(doc.paragraphs):
        if "START_CONTENT" in para.text:
            start_placeholder = i
        elif "END_CONTENT" in para.text:
            end_placeholder = i
            break
    
    if start_placeholder is None or end_placeholder is None:
        logging.error("Error: Could not find both placeholders")
        return

    for i in range(end_placeholder - 1, start_placeholder, -1):
        p = doc.paragraphs[i]
        p._element.getparent().remove(p._element)

    content_list = content_list[::-1]

    for content in content_list:
        if content['type'] == 'heading':
            doc.paragraphs[start_placeholder]._element.addnext(doc.add_heading(content['text'], level=1)._element)
        elif content['type'] == 'paragraph':
            doc.paragraphs[start_placeholder]._element.addnext(doc.add_paragraph(content['text'], style='Normal')._element)
        elif content['type'] == 'heading_2':
            doc.paragraphs[start_placeholder]._element.addnext(doc.add_heading(content['text'], level=2)._element)
        elif content['type'] == 'heading_4':
            doc.paragraphs[start_placeholder]._element.addnext(doc.add_heading(content['text'], level=4)._element)
        elif content['type'] == 'list_item':
            doc.paragraphs[start_placeholder]._element.addnext(doc.add_paragraph(content['text'], style='List Bullet')._element)

    doc.paragraphs[start_placeholder].text = ""
    doc.paragraphs[end_placeholder].text = ""

# Function to download template file from Google Docs
def download_template(url):
    # Modify the URL to force download as DOCX
    download_url = url.replace('/edit?usp=sharing', '/export?format=docx')
    try:
        response = requests.get(download_url)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download template: {e}")
        raise

# Function to check and insert scraped URLs into MongoDB
def check_and_insert_urls(urls):
    new_urls = []
    for url in urls:
        if not collection.find_one({'url': url}):
            new_urls.append(url)
            collection.insert_one({'url': url})
    return new_urls

# Function to send the PDF file to Telegram with retry mechanism
async def send_pdf_to_telegram(pdf_path, bot_token, channel_id):
    bot = telegram.Bot(token=bot_token)
    max_retries = 3
    
    # Get current date and format it
    current_date = datetime.now().strftime("%d %B %Y")
    file_name = f"{current_date} Current Affairs.pdf"
    
    for attempt in range(max_retries):
        try:
            with open(pdf_path, 'rb') as pdf_file:
                await bot.send_document(chat_id=channel_id, document=pdf_file, filename=file_name)
            logging.info(f"PDF sent to Telegram channel as '{file_name}'")
            break
        except telegram.error.TimedOut as e:
            logging.warning(f"Timeout error (attempt {attempt + 1}): {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error sending document (attempt {attempt + 1}): {e}")
            break

# Async function to handle main logic
async def main():
    try:
        base_url = "https://www.gktoday.in/current-affairs/"
        article_urls = fetch_article_urls(base_url, 2)
        new_urls = check_and_insert_urls(article_urls)
        if not new_urls:
            logging.info("No new articles found to scrape.")
            return
        
        template_url = os.getenv('TEMPLATE_URL')
        
        # Download the template
        template_bytes = download_template(template_url)
        
        doc = Document(template_bytes)
        logging.info("Template loaded successfully")
        
        all_content = []
        for url in new_urls:
            logging.info(f"Scraping: {url}")
            content_list = await scrape_and_get_content(url)
            all_content.extend(content_list)
        
        insert_content_between_placeholders(doc, all_content)
        
        # Create temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_docx:
            doc.save(tmp_docx.name)
        
        pdf_path = tmp_docx.name.replace('.docx', '.pdf')
        
        # Convert DOCX to PDF
        convert(tmp_docx.name, pdf_path)
        
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        channel_id = os.getenv('TELEGRAM_CHANNEL_ID')
        await send_pdf_to_telegram(pdf_path, bot_token, channel_id)
        
        # Clean up temporary files
        os.unlink(tmp_docx.name)
        os.unlink(pdf_path)
        
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
