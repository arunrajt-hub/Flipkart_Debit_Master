"""
HTML Table to Image Converter for n8n Workflow
Converts HTML tables to high-quality PNG images for WhatsApp sending

Usage:
    python html_table_to_image.py --html "<html>...</html>" --output "./images/table.png"
    python html_table_to_image.py --html-file "table.html" --output "./images/table.png"
    python html_table_to_image.py --html-stdin --output "./images/table.png"
"""

import argparse
import json
import os
import shutil
import sys
import time
import base64
import tempfile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image, ImageOps

def _resolve_chromedriver_path(explicit: str | None) -> str | None:
    """Prefer explicit arg, then CHROMEDRIVER / CHROMEDRIVER_PATH (e.g. GitHub Actions setup-chrome)."""
    for candidate in (
        explicit,
        os.environ.get("CHROMEDRIVER"),
        os.environ.get("CHROMEDRIVER_PATH"),
    ):
        if candidate and os.path.isfile(candidate):
            return candidate
    return None


def setup_chrome_driver(chromedriver_path=None):
    """Setup headless Chrome for screenshot. Pairs CHROME_BIN with matching driver; avoids WDM on CI."""
    chrome_options = Options()
    # Chrome 109+ / GitHub Actions: new headless mode is more stable than legacy --headless
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--force-device-scale-factor=2")  # High DPI
    chrome_options.add_argument("--disable-setuid-sandbox")  # Required in Docker/cloud

    # Cloud/Linux: set Chrome binary path (Render, Railway, GitHub Actions setup-chrome)
    for chrome_bin in [
        os.environ.get("CHROME_BIN"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ]:
        if chrome_bin and os.path.exists(chrome_bin):
            chrome_options.binary_location = chrome_bin
            break

    resolved = _resolve_chromedriver_path(chromedriver_path)
    if resolved:
        return webdriver.Chrome(service=Service(resolved), options=chrome_options)

    # No explicit driver: match installed Chrome. Do NOT prefer stale PATH/local chromedriver
    # (e.g. v143) before auto-matching — that causes "only supports Chrome version 143" errors.
    try:
        return webdriver.Chrome(service=Service(), options=chrome_options)
    except Exception:
        pass

    try:
        from webdriver_manager.chrome import ChromeDriverManager

        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=chrome_options
        )
    except Exception:
        pass

    service = None
    which_cd = shutil.which("chromedriver")
    if which_cd and os.path.isfile(which_cd):
        service = Service(which_cd)

    if not service:
        for path in (
            r"C:\Users\Lsn-Arun\Downloads\chromedriver-win64\chromedriver.exe",
            os.path.join(os.path.dirname(__file__), "chromedriver.exe"),
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
        ):
            if path and os.path.isfile(path):
                service = Service(path)
                break

    if not service:
        service = Service()

    return webdriver.Chrome(service=service, options=chrome_options)

def html_to_image(html_content, output_path, chromedriver_path=None, include_base64=False, raw_html=False, crop_selector=None):
    """
    Convert HTML to PNG image using Selenium
    
    Args:
        html_content: HTML string to convert
        output_path: Path where to save the PNG image
        chromedriver_path: Optional path to chromedriver executable
        include_base64: Whether to include base64 encoded image in output
        raw_html: If True, use html_content as-is (full document). If False, wrap in minimal template.
        crop_selector: CSS selector to crop to (e.g. ".container" or "table"). Captures only this element, trimming extra space.
        
    Returns:
        dict: Result information including image path, file size, etc.
    """
    driver = None
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # Setup Chrome driver
        driver = setup_chrome_driver(chromedriver_path)
        
        # Use content as-is for full documents (e.g. styled reports); else wrap in minimal template
        if raw_html and html_content.strip().lower().startswith('<!DOCTYPE'):
            full_html = html_content
        else:
            full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    margin: 0;
                    padding: 10px;
                    background-color: white;
                    font-family: Arial, sans-serif;
                }}
                table {{
                    border-collapse: collapse;
                    width: auto;
                    margin: 0 auto;
                }}
            </style>
        </head>
        <body>
            {html_content}
        </body>
        </html>
        """
        
        # Save HTML to a temporary file for better loading
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(full_html)
            temp_html_path = f.name
        
        try:
            # Load HTML from file (more reliable than data URI)
            driver.get(f"file:///{temp_html_path.replace(chr(92), '/')}")
            
            # Wait for rendering
            time.sleep(2)
            
            # Wait for table to be present (up to 10 seconds)
            table_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_html_path)
            except:
                pass
        
        # Use Chrome DevTools Protocol to capture BEYOND viewport (fixes truncated rows)
        page_height = driver.execute_script(
            "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);"
        )
        page_width = driver.execute_script(
            "return Math.max(document.body.scrollWidth, document.documentElement.scrollWidth, 1200);"
        )
        driver.set_window_size(int(page_width) + 50, min(int(page_height) + 100, 16384))
        time.sleep(0.5)

        temp_screenshot = output_path.replace('.png', '_temp.png')
        use_element_crop = False

        # Option A: Crop to specific element (e.g. .container) - captures only table, no extra space
        if crop_selector:
            try:
                crop_element = driver.find_element(By.CSS_SELECTOR, crop_selector)
                crop_element.screenshot(temp_screenshot)
                shutil.copy2(temp_screenshot, output_path)
                try:
                    os.remove(temp_screenshot)
                except OSError:
                    pass
                use_element_crop = True
            except Exception as crop_err:
                pass  # Fall through to full-page capture

        # Option B: Full-page capture with pixel-based crop
        if not use_element_crop:
            try:
                result_cdp = driver.execute_cdp_cmd("Page.captureScreenshot", {"captureBeyondViewport": True})
                if result_cdp and result_cdp.get("data"):
                    with open(temp_screenshot, "wb") as f:
                        f.write(base64.b64decode(result_cdp["data"]))
                else:
                    raise ValueError("CDP screenshot returned no data")
            except Exception as cdp_err:
                driver.save_screenshot(temp_screenshot)

            # Apply pixel-based crop (removes white margins)
            try:
                with Image.open(temp_screenshot) as img:
                    if img.mode != 'RGB':
                        img = img.convert('RGB')
                    gray = img.convert('L')
                    white_threshold = 252
                    mask = gray.point(lambda p: 255 if p < white_threshold else 0, mode='L')
                    bbox = mask.getbbox()
                    if bbox:
                        img = img.crop(bbox)
                    padding = 12
                    img = ImageOps.expand(img, border=padding, fill=(255, 255, 255))
                    img.save(output_path, 'PNG', quality=100, dpi=(300, 300))
            except Exception as crop_error:
                shutil.copy2(temp_screenshot, output_path)
            finally:
                if os.path.exists(temp_screenshot):
                    try:
                        os.remove(temp_screenshot)
                    except OSError:
                        pass
        
        # Get file size
        file_size = os.path.getsize(output_path)
        
        # Prepare result
        result = {
            "success": True,
            "image_path": os.path.abspath(output_path),
            "file_size": file_size,
            "file_size_kb": round(file_size / 1024, 2),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Add base64 if requested
        if include_base64:
            with open(output_path, 'rb') as img_file:
                result["image_base64"] = base64.b64encode(img_file.read()).decode('utf-8')
        
        return result
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    finally:
        if driver:
            driver.quit()

def main():
    """Main function to handle command-line execution"""
    parser = argparse.ArgumentParser(
        description='Convert HTML table to PNG image for n8n workflow'
    )
    
    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--html', type=str, help='HTML content as string')
    input_group.add_argument('--html-file', type=str, help='Path to HTML file')
    input_group.add_argument('--html-stdin', action='store_true', help='Read HTML from stdin')
    
    # Output options
    parser.add_argument('--output', type=str, required=True, help='Output PNG file path')
    parser.add_argument('--chromedriver', type=str, help='Path to chromedriver executable')
    parser.add_argument('--base64', action='store_true', help='Include base64 encoded image in output')
    parser.add_argument('--json-output', action='store_true', default=True, help='Output result as JSON')
    
    args = parser.parse_args()
    
    # Get HTML content from appropriate source
    if args.html:
        html_content = args.html
    elif args.html_file:
        if not os.path.exists(args.html_file):
            result = {
                "success": False,
                "error": f"HTML file not found: {args.html_file}",
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            print(json.dumps(result, indent=2))
            sys.exit(1)
        with open(args.html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
    else:  # stdin
        html_content = sys.stdin.read()
    
    # Validate HTML content
    if not html_content or not html_content.strip():
        result = {
            "success": False,
            "error": "HTML content is empty",
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        print(json.dumps(result, indent=2))
        sys.exit(1)
    
    # Convert HTML to image
    result = html_to_image(
        html_content=html_content,
        output_path=args.output,
        chromedriver_path=args.chromedriver,
        include_base64=args.base64
    )
    
    # Output result
    if args.json_output:
        print(json.dumps(result, indent=2))
    else:
        if result['success']:
            print(f"✅ Image created successfully: {result['image_path']}")
            print(f"📊 File size: {result['file_size_kb']} KB")
        else:
            print(f"❌ Error: {result['error']}")
            sys.exit(1)
    
    # Exit with appropriate code
    sys.exit(0 if result['success'] else 1)

if __name__ == "__main__":
    main()

