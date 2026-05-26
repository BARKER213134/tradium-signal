"""Скриншот кластерного графика с Resonance.vision.

Логинится через email/password, открывает кластер нужной пары, делает
скриншот ВСЕГО графика (не всего viewport — только chart canvas) и
возвращает PNG bytes.

Кэширует session_cookies между вызовами чтобы не логиниться каждый раз.
При неудаче скриншота возвращает None — endpoint вернёт JSON error.
"""
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

_RESONANCE_EMAIL = os.environ.get("RESONANCE_EMAIL", "bbeardy3@gmail.com")
_RESONANCE_PWD = os.environ.get("RESONANCE_PASSWORD", "1240Maxim")

# Кеш cookies + последнее время логина (re-login если старше 4 часов)
_session_cookies = None
_last_login_ts = 0
_SESSION_TTL_S = 4 * 3600  # 4h — потом перелогинимся


def _get_driver():
    """Headless Chromium driver. На Railway: CHROME_BIN=/usr/bin/chromium."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    opts.add_argument("--headless=new")  # new headless (supports WebGL via swiftshader)
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-logging")
    opts.add_argument("--log-level=3")
    # ── WebGL/GPU включаем через swiftshader (software rendering) ──
    # Resonance.vision требует WebGL — без этих флагов получаем
    # "График недоступен — Ваш браузер не поддерживает WebGL"
    # Угол (Angle) + swiftshader = software GL backend, работает в headless.
    opts.add_argument("--use-gl=angle")
    opts.add_argument("--use-angle=swiftshader")
    opts.add_argument("--enable-unsafe-swiftshader")  # forced swiftshader
    opts.add_argument("--enable-webgl")
    opts.add_argument("--enable-webgl2-compute-context")
    opts.add_argument("--ignore-gpu-blocklist")
    opts.add_argument("--enable-accelerated-2d-canvas")
    # Уменьшаем риск bot-detection
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    chrome_bin = os.environ.get("CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin

    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
    if chromedriver_path:
        service = Service(executable_path=chromedriver_path)
        d = webdriver.Chrome(service=service, options=opts)
    else:
        d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(30)
    return d


def _clear_popups(driver):
    """Удаляет все модали/туториалы/попапы которые могут блокировать UI."""
    try:
        driver.execute_script("""
            // Modal/Tour/Onboarding selectors
            document.querySelectorAll(
                '[class*="Modal"], [class*="Tour"], [class*="Onboarding"], ' +
                '[class*="cookie"], [class*="Cookie"], [class*="Welcome"], ' +
                '[class*="Ticker_modal"], [class*="Search_modal"], ' +
                '[class*="overlay"], [class*="Overlay"], [class*="popup"], ' +
                '[class*="Popup"], [class*="dialog"], [class*="Dialog"], ' +
                '[class*="Notification"], [class*="banner"], [class*="Banner"], ' +
                '[class*="LanguageSuggestion"]'
            ).forEach(e => { try { e.remove(); } catch(_) {} });
        """)
    except Exception:
        pass


def _login(driver):
    """Логин через email/password. Сохраняет cookies в module-level cache."""
    global _session_cookies, _last_login_ts
    from selenium.webdriver.common.by import By

    logger.info("[resonance] Starting login flow…")
    driver.get("https://resonance.vision/auth/login")
    time.sleep(6)
    _clear_popups(driver)
    time.sleep(1)

    # Switch to email login (default is OAuth tabs)
    try:
        driver.execute_script("""
            document.querySelectorAll('span, button, div').forEach(s => {
                const t = (s.textContent || '').trim();
                if (t === 'Email' || t === 'Email/Password' || t === 'Email & Password') {
                    try { s.click(); } catch(_) {}
                }
            });
        """)
    except Exception:
        pass
    time.sleep(3)

    # Fill email + password
    try:
        email_inp = driver.find_element(By.NAME, "email")
        email_inp.clear()
        email_inp.send_keys(_RESONANCE_EMAIL)
        pwd_inp = driver.find_element(By.NAME, "password")
        pwd_inp.clear()
        pwd_inp.send_keys(_RESONANCE_PWD)
    except Exception as e:
        logger.error(f"[resonance] login form not found: {e}")
        return False

    # Submit
    submitted = False
    for b in driver.find_elements(By.TAG_NAME, "button"):
        txt = (b.text or "").lower()
        if "sign" in txt or "log" in txt or "войти" in txt or "вход" in txt:
            try:
                driver.execute_script("arguments[0].click();", b)
                submitted = True
                break
            except Exception:
                continue
    if not submitted:
        # fallback: enter
        try:
            from selenium.webdriver.common.keys import Keys
            pwd_inp.send_keys(Keys.RETURN)
        except Exception:
            pass

    time.sleep(7)
    _session_cookies = driver.get_cookies()
    _last_login_ts = time.time()
    ok = "/clusters" in driver.current_url or "/dashboard" in driver.current_url or "/auth" not in driver.current_url
    logger.info(f"[resonance] login {'OK' if ok else 'PARTIAL'}, cookies={len(_session_cookies)}, url={driver.current_url}")
    return ok


def _apply_cookies(driver) -> bool:
    """Применяет сохранённые cookies, возвращает True если был login."""
    if not _session_cookies:
        return False
    try:
        driver.get("https://resonance.vision")
        time.sleep(2)
        for c in _session_cookies:
            try:
                # SameSite поле иногда мешает Selenium
                c_copy = {k: v for k, v in c.items() if k not in ('sameSite',)}
                driver.add_cookie(c_copy)
            except Exception:
                pass
        return True
    except Exception as e:
        logger.warning(f"[resonance] apply_cookies fail: {e}")
        return False


def _switch_pair(driver, sym_base: str) -> bool:
    """Переключает текущую пару (default обычно BTC). sym_base = 'ETH' / 'WIF' etc."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys

    if sym_base.upper() == "BTC":
        return True

    target_full = f"{sym_base.upper()}/USDT"
    # Кликаем по текущей паре (обычно "BTC/USDT" в кнопке/баре)
    clicked = False
    for b in driver.find_elements(By.TAG_NAME, "button"):
        if "/USDT" in (b.text or ""):
            try:
                driver.execute_script("arguments[0].click();", b)
                clicked = True
                break
            except Exception:
                continue
    if not clicked:
        # Может быть текстовая кнопка в баре или search icon
        for el in driver.find_elements(By.CSS_SELECTOR, '[class*="search"], [class*="Search"], [class*="ticker"], [class*="Ticker"]'):
            try:
                if el.is_displayed():
                    driver.execute_script("arguments[0].click();", el)
                    clicked = True
                    break
            except Exception:
                continue
    time.sleep(2)
    _clear_popups(driver)
    # Search input
    inputs = driver.find_elements(By.TAG_NAME, "input")
    typed = False
    for inp in inputs:
        try:
            if inp.is_displayed():
                inp.clear()
                inp.send_keys(sym_base)
                typed = True
                time.sleep(3)
                break
        except Exception:
            continue
    if not typed:
        return False

    # Click first matching result
    results = driver.find_elements(By.XPATH, f"//*[contains(text(), '{target_full}')]")
    for r in results:
        try:
            if r.is_displayed() and r.tag_name not in ('button', 'input', 'html', 'body'):
                driver.execute_script("arguments[0].click();", r)
                time.sleep(3)
                return True
        except Exception:
            continue
    # Если не нашли — пробуем нажать enter
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.RETURN)
        time.sleep(3)
    except Exception:
        pass
    return False


def _set_timeframe(driver, tf_label: str):
    """Выбирает таймфрейм (M15/M30/H1/H4)."""
    from selenium.webdriver.common.by import By
    for b in driver.find_elements(By.TAG_NAME, "button"):
        try:
            if (b.text or "").strip() == tf_label and b.is_displayed():
                driver.execute_script("arguments[0].click();", b)
                time.sleep(2)
                return True
        except Exception:
            continue
    return False


def _capture_chart_only(driver) -> Optional[bytes]:
    """Пытается найти canvas/chart-container и сделать его cropped screenshot.
    Fallback: full viewport screenshot."""
    from selenium.webdriver.common.by import By
    # Сначала пытаемся найти chart canvas
    selectors = [
        'canvas',
        '[class*="ChartContainer"]',
        '[class*="chart-container"]',
        '[class*="Chart_"]',
        '[id*="chart"]',
        'main',
    ]
    for sel in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in elements:
                if el.is_displayed():
                    size = el.size
                    if size['width'] >= 600 and size['height'] >= 400:
                        try:
                            png = el.screenshot_as_png
                            if png and len(png) > 5000:  # хотя бы 5kb
                                return png
                        except Exception:
                            continue
        except Exception:
            continue
    # Fallback — full viewport
    try:
        return driver.get_screenshot_as_png()
    except Exception:
        return None


def get_cluster_screenshot(symbol: str, timeframe: str = "H1") -> Optional[bytes]:
    """Делает скриншот кластерного графика с Resonance.vision.

    Args:
        symbol: 'ETHUSDT' или 'ETH/USDT' или 'ETH'
        timeframe: 'M15' / 'M30' / 'H1' / 'H4' (или нижний регистр '15m'/'1h'/'4h')

    Returns:
        PNG bytes или None при ошибке.
    """
    sym_base = symbol.upper().replace("/USDT", "").replace("USDT", "").replace("/", "")
    tf_map = {"M15": "M15", "M30": "M30", "H1": "H1", "H4": "H4",
              "15m": "M15", "30m": "M30", "1h": "H1", "4h": "H4"}
    tf_label = tf_map.get(timeframe, "H1")

    driver = None
    try:
        driver = _get_driver()

        # ── Step 1: login if needed ──────────────────────────────────
        global _session_cookies, _last_login_ts
        session_expired = (time.time() - _last_login_ts) > _SESSION_TTL_S
        if _session_cookies and not session_expired:
            _apply_cookies(driver)
            driver.get("https://resonance.vision/clusters")
            time.sleep(5)
            if "/auth" in driver.current_url.lower():
                logger.info("[resonance] cookies expired, re-login")
                _session_cookies = None
                _login(driver)
                driver.get("https://resonance.vision/clusters")
                time.sleep(6)
        else:
            _login(driver)
            driver.get("https://resonance.vision/clusters")
            time.sleep(8)

        _clear_popups(driver)
        time.sleep(1)

        # Close any tutorial buttons
        from selenium.webdriver.common.by import By
        for b in driver.find_elements(By.TAG_NAME, "button"):
            try:
                txt = (b.text or "").strip().lower()
                if txt in ('skip', 'close', 'got it', 'ok', 'no, thanks',
                           'пропустить', 'закрыть', 'хорошо', 'не сейчас'):
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(1)
            except Exception:
                continue
        _clear_popups(driver)
        time.sleep(1)

        # ── Step 2: switch pair ───────────────────────────────────────
        _switch_pair(driver, sym_base)
        _clear_popups(driver)
        time.sleep(1)

        # ── Step 3: set timeframe ─────────────────────────────────────
        _set_timeframe(driver, tf_label)
        _clear_popups(driver)
        # Ждём финального ре-рендера chart
        time.sleep(4)
        _clear_popups(driver)

        # ── Step 4: screenshot (chart-only по возможности, иначе full) ─
        png = _capture_chart_only(driver)
        if png:
            logger.info(f"[resonance] screenshot OK: {symbol} {tf_label} ({len(png)} bytes)")
        else:
            logger.warning(f"[resonance] screenshot empty: {symbol} {tf_label}")
        return png

    except Exception as e:
        logger.error(f"[resonance] screenshot crashed: {e}", exc_info=True)
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
