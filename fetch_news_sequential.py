import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from lxml import html
import pandas as pd
import signal

BASE = "https://www.investing.com"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\n\n⚠️  Shutdown signal received. Finishing current operation...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)


# -------------------------------------------------
# Warm up session + accept cookies
# -------------------------------------------------
async def warm_session(page):
    try:
        await page.goto(BASE, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        try:
            await page.click("button:has-text('I Accept')", timeout=5000)
            await page.wait_for_timeout(1000)
        except:
            pass
    except Exception as e:
        print(f"  ⚠️  Warning: Could not warm session: {e}")


# -------------------------------------------------
# Build pagination URL
# -------------------------------------------------
def build_news_page(base_url: str, page_no: int) -> str:
    base_url = base_url.rstrip("/")
    return base_url if page_no == 1 else f"{base_url}/{page_no}"


# -------------------------------------------------
# Extract article links from listing page
# -------------------------------------------------
async def get_article_links(page, url):
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)

        await page.wait_for_function(
            "() => document.querySelectorAll('article[data-test=\"article-item\"]').length > 0",
            timeout=20000
        )

        tree = html.fromstring(await page.content())
        links = tree.xpath(
            "//article[@data-test='article-item']//a[@data-test='article-title-link']/@href"
        )

        dates = tree.xpath(
            "//article[@data-test='article-item']//time/@datetime"
        )

        clean = []
        for l, d in zip(links, dates):
            if l.startswith("/"):
                l = BASE + l
            clean.append((l, d))

        seen = {}
        for l, d in clean:
            seen[l] = d

        return list(seen.items())
    except Exception:
        return []


# -------------------------------------------------
# Extract article headline + body
# -------------------------------------------------
async def get_article_content(page, url, timeout=25000):
    try:
        # 1. Navigation with a more realistic wait
        await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
        
        # 2. Critical: Wait for the article container to actually exist
        try:
            await page.wait_for_selector("h1, .articleHeader", timeout=10000)
        except:
            return None  # Page failed to load or bot-blocked

        # 3. Get the full HTML content
        content = await page.content()
        tree = html.fromstring(content)

        # 4. Flexible Headline Extraction
        headline_paths = [
            "//h1/text()",
            "//h1[contains(@class, 'articleHeader')]/text()",
            "//h1[@id='articleTitle']/text()",
            "//div[contains(@class,'article-header')]//h1/text()"
        ]
        
        headline = ""
        for path in headline_paths:
            result = tree.xpath(path)
            if result:
                headline = result[0].strip()
                break

        # 5. Flexible Body Extraction
        body_containers = [
            "//div[contains(@class, 'WYSIWYG')]",
            "//div[@id='articleContent']",
            "//div[contains(@class, 'articlePage')]",
            "//div[@class='article-content']"
        ]
        
        body_parts = []
        for container_path in body_containers:
            paragraphs = tree.xpath(f"{container_path}//p//text()")
            if paragraphs:
                body_parts = [p.strip() for p in paragraphs if len(p.strip()) > 10]
                if body_parts:
                    break

        body = " ".join(body_parts)

        # 6. Final Validation - check if it's actually an article
        if headline and len(body) > 100:
            return {
                "article_url": url,
                "headline": headline,
                "body": body
            }
        return None

    except Exception:
        return None


# -------------------------------------------------
# Process a single page sequentially
# -------------------------------------------------
async def process_single_page(page, base_url, page_no, skip_pro=True):
    page_url = build_news_page(base_url, page_no)
    results = []
    
    try:
        # Get article links
        link_date_pair = await get_article_links(page, page_url)
        
        if not link_date_pair:
            return results, 0
        
        # Filter out pro articles
        if skip_pro:
            link_date_pair = [(l, d) for l, d in link_date_pair if "/news/pro/" not in l]
        
        if not link_date_pair:
            return results, 0
        
        # Extract articles one by one (sequentially)
        for article_url, article_date in link_date_pair:
            if shutdown_flag:
                break
                
            article_data = await get_article_content(page, article_url)
            
            if article_data and isinstance(article_data, dict):
                article_data["company_news_url"] = base_url
                article_data["published_date"] = article_date
                results.append(article_data)
            
            # Small delay between articles
            await asyncio.sleep(0.3)
        
        return results, len(link_date_pair)
    
    except Exception as e:
        print(f"  ⚠️  Error processing page {page_no}: {e}")
        return results, 0


# -------------------------------------------------
# Main sequential runner
# -------------------------------------------------
async def run_async(
    csv_path, 
    max_pages=None, 
    skip_pro=True,
    save_interval=100
):
    """
    Sequential scraper - processes one page and one article at a time.
    
    Args:
        csv_path: Path to CSV with news URLs
        max_pages: Maximum pages to scrape per URL (None = unlimited)
        skip_pro: Skip /news/pro/ articles
        save_interval: Save progress after this many articles
    """
    global shutdown_flag
    
    df = pd.read_csv(csv_path)
    all_results = []
    
    # Track processed articles globally to prevent duplicates
    seen_article_urls = set()
    
    unique_urls = df["news_url"].dropna().unique()
    
    print(f"📋 Found {len(unique_urls)} unique URLs to crawl")
    print(f"⚙️  Sequential Processing (one article at a time)")
    print(f"   • Skip pro articles: {skip_pro}\n")

    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                executable_path=CHROME_PATH,
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-gpu",
                ]
            )

            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                viewport={"width": 1366, "height": 768}
            )

            # Create a single page for warm session
            warm_page = await context.new_page()
            await warm_session(warm_page)
            await warm_page.close()

            # Create a single page to reuse throughout
            page = await context.new_page()
            total_articles = 0

            for idx, base_url in enumerate(unique_urls, 1):
                if shutdown_flag:
                    print("\n⚠️  Shutdown requested, stopping...")
                    break
                
                if base_url.startswith("/"):
                    base_url = BASE + base_url
                elif not base_url.startswith("http"):
                    base_url = "https://" + base_url

                print(f"▶ [{idx}/{len(unique_urls)}] Crawling: {base_url}")

                company_articles = 0
                page_no = 1
                
                while not shutdown_flag:
                    if max_pages is not None and page_no > max_pages:
                        print(f"  ✓ Reached max pages limit ({max_pages})")
                        break
                    
                    # Process one page at a time
                    page_results, num_links = await process_single_page(
                        page, base_url, page_no, skip_pro
                    )
                    
                    # Filter out duplicate articles
                    new_results = []
                    for article_data in page_results:
                        article_url = article_data["article_url"]
                        if article_url not in seen_article_urls:
                            seen_article_urls.add(article_url)
                            new_results.append(article_data)
                    
                    if new_results:
                        all_results.extend(new_results)
                        company_articles += len(new_results)
                        total_articles += len(new_results)
                    
                    if num_links > 0:
                        print(f"  ✓ Page {page_no}: {len(new_results)}/{num_links} articles extracted")
                    
                    # Auto-save progress
                    if total_articles > 0 and total_articles % save_interval == 0:
                        temp_df = pd.DataFrame(all_results)
                        temp_df.to_csv("news_sentiment_data_temp.csv", index=False)
                        print(f"  💾 Progress saved: {total_articles} total articles")
                    
                    # If no links found, stop pagination
                    if num_links == 0:
                        print(f"  ✓ No more articles found, pagination complete")
                        break
                    
                    # Move to next page
                    page_no += 1
                    await asyncio.sleep(0.5)

                print(f"  ✅ Company total: {company_articles} unique articles\n")

    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n🧹 Cleaning up resources...")
        if page:
            try:
                await page.close()
            except:
                pass
        if context:
            try:
                await context.close()
            except:
                pass
        if browser:
            try:
                await browser.close()
            except:
                pass

    print(f"\n{'='*60}")
    print(f"📊 FINAL SUMMARY:")
    print(f"   Total unique articles scraped: {total_articles}")
    print(f"   Total article URLs seen: {len(seen_article_urls)}")
    print(f"{'='*60}")

    return pd.DataFrame(all_results)


# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting SEQUENTIAL web scraper...")
    print("   (Processes one page and one article at a time)")
    print("   Press Ctrl+C to stop gracefully\n")
    
    try:
        df_news = asyncio.run(
            run_async(
                "url_sample.csv", 
                max_pages=None,          
                skip_pro=True,        
                save_interval=100     
            )
        )
        
        if len(df_news) > 0:
            # Final deduplication check
            df_news = df_news.drop_duplicates(subset=['article_url'], keep='first')
            
            df_news.to_csv("news_sentiment_data.csv", index=False)
            print(f"\n✅ Saved to: news_sentiment_data.csv")
            print(f"   Total unique articles: {len(df_news)}")
        else:
            print("\n⚠️  No articles were scraped successfully")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()