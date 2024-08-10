#!/usr/bin/env python3

def chromedriver():
    """Use to create new token when getting wonky login issues,
    like bad code 400 or Login denied.

    When creating new tokens, delete appropriate token file
    MAKE SURE to login with the appropriate login credentials

    """
    import atexit
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager

    driver = webdriver.Chrome(ChromeDriverManager().install())
    atexit.register(lambda: driver.quit())
    return driver
