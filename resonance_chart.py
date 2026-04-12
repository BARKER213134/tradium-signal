"""Скриншот кластерного графика с Resonance.vision.

Логинится через email/password, открывает кластер нужной пары,
делает скриншот и возвращает PNG bytes.
"""
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

_RESONANCE_EMAIL = "bbeardy3@gmail.com"
_RESONANCE_PWD = "1240Maxim"

# Кеш cookies чтобы не логиниться каждый раз
_session_cookies = None


def _get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-logging")
    opts.add_argument("--log-level=3")
    return webdriver.Chrome(options=opts)


def _login(driver):
    """Логин через email/password. Сохраняет cookies."""
    global _session_cookies
    from selenium.webdriver.common.by import By

    driver.get("https://resonance.vision/auth/login")
    time.sleep(6)

    # Убираем модали
    driver.execute_script(
        "document.querySelectorAll('[class*=\"Modal_overlay\"], [class*=\"LanguageSuggestion\"]').forEach(e => e.remove());"
    )
    time.sleep(1)

    # Кликаем Email
    driver.execute_script(
        "document.querySelectorAll('span').forEach(s => { if (s.textContent.trim() === 'Email') s.click(); });"
    )
    time.sleep(3)

    # Заполняем
    driver.find_element(By.NAME, "email").send_keys(_RESONANCE_EMAIL)
    driver.find_element(By.NAME, "password").send_keys(_RESONANCE_PWD)

    # Submit
    for b in driver.find_elements(By.TAG_NAME, "button"):
        if "sign" in b.text.lower():
            driver.execute_script("arguments[0].click();", b)
            break
    time.sleep(6)

    # Сохраняем cookies
    _session_cookies = driver.get_cookies()
    logger.info(f"Resonance login OK, {len(_session_cookies)} cookies")


def _apply_cookies(driver):
    """Применяет сохранённые cookies."""
    if not _session_cookies:
        return False
    driver.get("https://resonance.vision")
    time.sleep(2)
    for c in _session_cookies:
        try:
            driver.add_cookie(c)
        except Exception:
            pass
    return True


def get_cluster_screenshot(symbol: str, timeframe: str = "H1") -> Optional[bytes]:
    """Делает скриншот кластерного графика с Resonance.

    Args:
        symbol: e.g. "ETHUSDT" or "ETH/USDT"
        timeframe: "M15", "H1", "H4" etc.

    Returns:
        PNG bytes or None on error
    """
    from selenium.webdriver.common.by import By

    pair = symbol.replace("USDT", "").replace("/", "") + "/USDT"
    sym_search = symbol.replace("/", "").replace("USDT", "")

    driver = None
    try:
        driver = _get_driver()

        # Пробуем с cookies
        if _session_cookies:
            _apply_cookies(driver)
            driver.get("https://resonance.vision/clusters")
            time.sleep(5)
            # Проверяем залогинены ли
            if "login" in driver.current_url.lower():
                _login(driver)
                driver.get("https://resonance.vision/clusters")
                time.sleep(5)
        else:
            _login(driver)
            driver.get("https://resonance.vision/clusters")
            time.sleep(8)

        # Убираем попапы
        driver.execute_script("""
            document.querySelectorAll(
                '[class*="Modal"], [class*="Tour"], [class*="Onboarding"], ' +
                '[class*="cookie"], [class*="Cookie"], [class*="Welcome"]'
            ).forEach(e => e.remove());
        """)
        time.sleep(1)

        # Скип туториал
        for b in driver.find_elements(By.TAG_NAME, "button"):
            if "skip" in b.text.lower():
                driver.execute_script("arguments[0].click();", b)
                time.sleep(1)
                break

        # Смена пары если не BTC
        if sym_search.upper() != "BTC":
            # Кликаем на search
            for b in driver.find_elements(By.TAG_NAME, "button"):
                if "/USDT" in b.text:
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(2)
                    break

            # Ищем input
            inputs = driver.find_elements(By.TAG_NAME, "input")
            for inp in inputs:
                if inp.is_displayed():
                    inp.clear()
                    inp.send_keys(sym_search)
                    time.sleep(2)
                    # Кликаем результат
                    target = f"{sym_search.upper()}/USDT"
                    results = driver.find_elements(By.XPATH, f"//*[contains(text(), '{target}')]")
                    for r in results:
                        if r.is_displayed():
                            driver.execute_script("arguments[0].click();", r)
                            time.sleep(4)
                            break
                    break

        # Выбираем таймфрейм
        tf_map = {"M15": "M15", "M30": "M30", "H1": "H1", "H4": "H4", "1h": "H1", "4h": "H4", "15m": "M15"}
        tf_label = tf_map.get(timeframe, "H1")
        for b in driver.find_elements(By.TAG_NAME, "button"):
            if b.text.strip() == tf_label:
                driver.execute_script("arguments[0].click();", b)
                time.sleep(3)
                break

        # Убираем sidebar для чистого графика
        driver.execute_script("""
            document.querySelectorAll('[class*="Sidebar"], [class*="sidebar"], [class*="cookie"]').forEach(e => e.style.display='none');
        """)
        time.sleep(1)

        # Скриншот
        png = driver.get_screenshot_as_png()
        logger.info(f"Resonance screenshot: {symbol} {timeframe} ({len(png)} bytes)")
        return png

    except Exception as e:
        logger.error(f"Resonance screenshot error: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
