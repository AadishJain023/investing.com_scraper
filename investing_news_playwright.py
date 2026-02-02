import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from lxml import html
import pandas as pd
import signal
from collections import defaultdict

BASE = "https://www.investing.com"
CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\n\n⚠️  Shutdown signal received. Finishing current batch...")
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

        clean = []
        for l in links:
            if l.startswith("/"):
                l = BASE + l
            clean.append(l)

        return list(dict.fromkeys(clean))
    except Exception:
        return []


# -------------------------------------------------
# Extract article headline + body
# -------------------------------------------------
async def get_article_content(context, url, semaphore, timeout=25000):
    async with semaphore:
        page = None
        try:
            page = await context.new_page()
            await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            await page.wait_for_timeout(800)

            try:
                await page.wait_for_selector("h1, h2.article-title", timeout=8000)
            except PlaywrightTimeout:
                return None

            tree = html.fromstring(await page.content())

            headline = (
                tree.xpath("//h1/text()") or 
                tree.xpath("//h2[@class='article-title']/text()") or
                tree.xpath("//div[@class='article-header']//h1/text()")
            )
            headline = headline[0].strip() if headline else ""

            body_texts = (
                tree.xpath("//div[contains(@class,'article')]//p/text()") or
                tree.xpath("//div[@class='article-content']//p/text()") or
                tree.xpath("//div[contains(@class,'articlePage')]//p/text()")
            )
            
            body = " ".join(p.strip() for p in body_texts if p.strip())

            if headline and body and len(body) > 50:
                return {
                    "article_url": url,
                    "headline": headline,
                    "body": body
                }
            return None

        except Exception:
            return None
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass


# -------------------------------------------------
# Worker: Processes a single page with parallel article extraction
# -------------------------------------------------
async def process_single_page(context, base_url, page_no, article_semaphore, skip_pro=True):
    page_url = build_news_page(base_url, page_no)
    
    list_page = await context.new_page()
    results = []
    
    try:
        # Get article links
        links = await get_article_links(list_page, page_url)
        
        if not links:
            return results, 0, page_no
        
        # Filter out pro articles
        if skip_pro:
            links = [l for l in links if "/news/pro/" not in l]
        
        if not links:
            return results, 0, page_no
        
        # Extract all articles in parallel
        tasks = [
            get_article_content(context, article_url, article_semaphore)
            for article_url in links
        ]
        
        article_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful extractions
        for article_data in article_results:
            if article_data and isinstance(article_data, dict):
                article_data["company_news_url"] = base_url
                results.append(article_data)
        
        return results, len(links), page_no
    
    finally:
        await list_page.close()


# -------------------------------------------------
# Main async runner - FIXED DUPLICATE PREVENTION
# -------------------------------------------------
async def run_async(
    csv_path, 
    max_pages=None, 
    num_page_workers=3,
    articles_per_page=8,
    skip_pro=True,
    save_interval=100
):
    """
    Fixed hybrid parallel scraper with proper page tracking.
    
    Args:
        csv_path: Path to CSV with news URLs
        max_pages: Maximum pages to scrape per URL (None = unlimited)
        num_page_workers: Number of pages to process simultaneously
        articles_per_page: Concurrent article extractions per page
        skip_pro: Skip /news/pro/ articles
        save_interval: Save progress after this many articles
    """
    global shutdown_flag
    
    df = pd.read_csv(csv_path)
    all_results = []
    
    # ✅ Track processed articles globally to prevent duplicates
    seen_article_urls = set()
    
    unique_urls = df["news_url"].dropna().unique()
    total_concurrency = num_page_workers * articles_per_page
    
    print(f"📋 Found {len(unique_urls)} unique URLs to crawl")
    print(f"⚙️  Parallel Configuration:")
    print(f"   • {num_page_workers} pages processed simultaneously")
    print(f"   • {articles_per_page} articles per page extracted in parallel")
    print(f"   • Total concurrency: {total_concurrency} operations")
    print(f"   • Skip pro articles: {skip_pro}\n")

    browser = None
    context = None
    
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

            warm_page = await context.new_page()
            await warm_session(warm_page)
            await warm_page.close()

            article_semaphore = asyncio.Semaphore(articles_per_page)
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
                
                # ✅ Track which pages we've already processed for this company
                processed_pages = set()
                
                while not shutdown_flag:
                    if max_pages is not None and page_no > max_pages:
                        print(f"  ✓ Reached max pages limit ({max_pages})")
                        break
                    
                    # ✅ Create batch of page numbers that haven't been processed yet
                    page_batch = []
                    for p in range(page_no, page_no + num_page_workers):
                        if p not in processed_pages:
                            page_batch.append(p)
                    
                    if not page_batch:
                        break
                    
                    # ✅ Mark these pages as being processed
                    for p in page_batch:
                        processed_pages.add(p)
                    
                    # Process pages in parallel
                    tasks = [
                        process_single_page(context, base_url, p, article_semaphore, skip_pro)
                        for p in page_batch
                    ]
                    
                    # ✅ Wait for ALL tasks to complete before continuing
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    batch_articles = 0
                    batch_had_content = False
                    batch_stats = []
                    
                    for result in batch_results:
                        if isinstance(result, Exception):
                            continue
                        
                        page_results, num_links, processed_page = result
                        
                        if num_links > 0:
                            batch_had_content = True
                        
                        # ✅ Filter out duplicate articles
                        new_results = []
                        for article_data in page_results:
                            article_url = article_data["article_url"]
                            if article_url not in seen_article_urls:
                                seen_article_urls.add(article_url)
                                new_results.append(article_data)
                        
                        if new_results:
                            all_results.extend(new_results)
                            batch_articles += len(new_results)
                            company_articles += len(new_results)
                            total_articles += len(new_results)
                        
                        # Store stats for this page
                        batch_stats.append({
                            'page': processed_page,
                            'extracted': len(new_results),
                            'total': num_links
                        })
                    
                    # ✅ Print results sorted by page number
                    batch_stats.sort(key=lambda x: x['page'])
                    for stat in batch_stats:
                        if stat['total'] > 0:
                            print(f"  ✓ Page {stat['page']}: {stat['extracted']}/{stat['total']} articles extracted")
                    
                    # Auto-save progress
                    if total_articles > 0 and total_articles % save_interval == 0:
                        temp_df = pd.DataFrame(all_results)
                        temp_df.to_csv("news_sentiment_data_temp.csv", index=False)
                        print(f"  💾 Progress saved: {total_articles} total articles")
                    
                    # If no content in this batch, stop
                    if not batch_had_content:
                        print(f"  ✓ No more articles found, pagination complete")
                        break
                    
                    # ✅ Move to next batch AFTER current batch completes
                    page_no += num_page_workers
                    await asyncio.sleep(0.5)

                print(f"  ✅ Company total: {company_articles} unique articles\n")

    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n🧹 Cleaning up resources...")
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
    print("🚀 Starting HYBRID PARALLEL web scraper (FIXED)...")
    print("   (Parallelizes pages AND articles with duplicate prevention)")
    print("   Press Ctrl+C to stop gracefully\n")
    
    try:
        df_news = asyncio.run(
            run_async(
                "url_sample.csv", 
                max_pages=None,          
                num_page_workers=3,      
                articles_per_page=8,     
                skip_pro=True,        
                save_interval=100     
            )
        )
        
        if len(df_news) > 0:
            # ✅ Final deduplication check
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