from selenium import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.relative_locator import locate_with
from exception import CannotCaculateFinalPrice
import time
import pandas as pd

class CrawlFood:

    def __init__(self, profile: str = None, dir_path: str = None, port: int = None) -> None:
        
        """
        dir_path: path of profile directory
        profile : 'Profile 1'
        dir_path: 'C:/Users/luong/AppData/Local/Google/Chrome/User Data'
        """

        self.options = webdriver.ChromeOptions()
        if dir_path is not None:
            self.options.add_argument("--user-data-dir=" + dir_path)
        if profile is not None:
            self.options.add_argument('--profile-directory=' + profile)
        if port is not None:
            self.options.add_argument('--remote-debugging-port={}'.format(port))
        self.driver = webdriver.Chrome(options = self.options)
        self.action_chain = ActionChains(self.driver)
        self.driver.implicitly_wait(5)

    def get_link(self, url: str = 'https://vi-vn.facebook.com/'):
        self.driver.get(url)
        self.driver.maximize_window()

    def get_infor(self, category):
        name_path = '//h1[@class="product-detailsstyle__ProductTitle-sc-127s7qc-8 gWYkcq"]'
        SKU_path = '//p[@class="product-detailsstyle__ProductSku-sc-127s7qc-9 kitwDR"]'
        status_price_path = '//div[@class="product-detailsstyle__ProductNamePrice-sc-127s7qc-23 bIvxaa"]'
        price_discount = '//div[@class="product-detailsstyle__ProductPrice-sc-127s7qc-22 fMAYId fs-15"]'
        price_path = '//div[@class="product-detailsstyle__ProductPrice-sc-127s7qc-22 fMAYId  fs-15"]'
        type_path = '//div[@class="box__Box-sc-1h894p0-0 ZdIyw box"]'

        name = self.driver.find_element(By.XPATH, name_path).text
        sku = self.driver.find_element(By.XPATH, SKU_path).text.split('\n')[0][5:]
        status_price = self.driver.find_element(By.XPATH, status_price_path).text
        
        if status_price == 'Giá niêm yết':
            price = int(self.driver.find_element(By.XPATH, price_discount).text[:-1].replace('.', ''))
        else:
            price = int(self.driver.find_element(By.XPATH, price_path).text[:-1].replace('.', ''))
        kind = self.driver.find_element(By.XPATH, type_path).text
        
        # nếu cách tính là kg hoặc gram
        if kind[0].isdigit():
            # nếu là 1kg (quy đổi về giá 1kg)
            if kind[0] == '1' and kind[1] != '.':
                final_price = price
            else:
                final_price = round(price / float(kind[:-2]), 2)
        # nếu cách tính là khay hoặc hộp
        else:
            try:
                if name[-4:-1].lower() == 'kg':
                    unit = name[-3]
                    final_price = round(price / int(unit), 2)
                else:
                    unit = name[-4:-1]
                    final_price = round(price / (int(unit) / 1000), 2)
            except:
                final_price = None
                pass
                # raise CannotCaculateFinalPrice('Cannot find unit of measurement')

        return {'sku': [sku], 'category': [category],'name': [name], 'price': [final_price]}

    def get_product(self, df, category):
        x_path_grid = '//div[@class="product-card-two__Card-sc-1lvbgq2-0 kbnZVu product-card"]'
        self.action_chain.scroll_by_amount(0, 2000).perform()
        time.sleep(2)
        products = self.driver.find_elements(By.XPATH, x_path_grid)
        print(len(products))

        for product in products:
            self.action_chain.key_down(Keys.CONTROL).click(product).key_up(Keys.CONTROL).perform()
            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            info = self.get_infor(category)
            info = pd.DataFrame(info)
            df = pd.concat([df, info], ignore_index=True)

            self.driver.execute_script("window.close();")
            self.driver.switch_to.window(self.driver.window_handles[-1])

        return df

