#!/usr/bin/python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

URL_genomes = 'https://www.ncbi.nlm.nih.gov/variation/tools/1000genomes/'
URL_nucleotide = 'https://blast.ncbi.nlm.nih.gov/Blast.cgi?PAGE=MegaBlast&PROGRAM=blastn&PAGE_TYPE=BlastSearch&BLAST_SPEC=OGP__9606__9558'


class WebWorker(object):
    
    def __init__(self, url_genomes=URL_genomes, url_nucleotide=URL_nucleotide):
        driver = webdriver.Chrome()
        # driver.implicitly_wait(10)
        # driver.set_window_size(1280, 960)
        # driver.maximize_window()
        self.driver = driver
        # driver.execute_script("window.open('');")
        self.url_nucleotide = url_nucleotide
        self.url_genomes = url_genomes

        self.win_genomes_idx = 0
        self.win_nucleotide_idx = 1
        
        self.load_page(url_genomes, self.win_genomes_idx)

        self.page_genomes_title = '1000 Genomes Browser'
        self.page_nucleotide_title = 'Nucleotide BLAST'
        self.input_box_xpath = '//*[@id="loc-search"]'
        self.zoomout_xpath = "//a[@class='x-btn x-unselectable x-box-item x-toolbar-item x-btn-default-toolbar-small'][5]"
        self.page_div_xpath = '//*[@id="svw-1"]'
        self.canvas_xpath = '//canvas[@class="sv-drag sv-highlight sv-dblclick"][2]'
        self.blast_select_1_xpath = '//span[@class="x-menu-item-text x-menu-item-text-default x-menu-item-indent x-menu-item-indent-right-arrow" and contains(text(), "BLAST and Primer Search")]'
        self.blast_select_2_xpath = '//span[@class="x-menu-item-text x-menu-item-text-default x-menu-item-indent" and contains(text(), "BLAST Search (Visible Range)")]'
        self.text_area_xpath = '//*[@id="seq"]'
        self.from_box_xpath = '//*[@id="QUERY_FROM"]'
        self.to_box_xpath = '//*[@id="QUERY_TO"]'
        self.database_select_xpath = '//*[@id="DATABASE"]/option[14]'
        self.result_table_xpath =  '//*[@id="dscTable"]'
        self.input_button_class_name = 'gb-search-go'
        self.blast_button_xpath = '//*[@id="b1"]'
        self.load_variable_xpath = '//*[@id="ui-ncbiexternallink-1"]/div[2]/div[5]/div[3]/div[2]/div/div[2]/div[3]/span/span'
        self.loading_xpath = '//*[@id="ui-ncbiexternallink-1"]'
        self.loading_div_xpath = '//div[@class="x-toolbar x-docked x-toolbar-default x-docked-bottom x-toolbar-docked-bottom x-toolbar-default-docked-bottom x-box-layout-ct"][1]'
        self.popup_box_xpath = '//div[@class="x-menu x-layer x-menu-default x-body x-border-box"][3]'
        self.reult_panel_xpath = '//*[@id="alignView"]'


    def __call__(self, input_data):
        data = self.parse_data(input_data)
        result = self.parse_result(data)
        return result

    def load_page(self, url, win_idx, timeout_sec=60):
        try:
            self.driver.set_page_load_timeout(timeout_sec)
            nrof_win = len(self.driver.window_handles)
            assert win_idx >= 0 and win_idx <= nrof_win-1, 'Number of windows is not %s, but %s'%(win_idx+1, nrof_win)
            self.driver.switch_to_window(self.driver.window_handles[win_idx])
            self.driver.get(url)
        except TimeoutException:
            print('Timeout when try to connect url: ', url)
            sys.exit(-1)

    def get_element(self, xpath, timeout_sec=1, visible=False, clickable=False, other_ec_list=None):
        ec_list = [EC.presence_of_element_located((By.XPATH, xpath))]

        if visible:
            ec_list.append(EC.visibility_of_element_located((By.XPATH, xpath)))

        if clickable:
            ec_list.append(EC.element_to_be_clickable((By.XPATH, xpath)))

        if other_ec_list is not None:
            ec_list += other_ec_list
            
        if len(ec_list) == 0:
            ec_list = None

        waiter = WebDriverWait(self.driver, timeout_sec)
        element = None
        for ec in ec_list:
            try:
                element = waiter.until(ec, message='timeout for ec=%s'%ec)
            except TimeoutException:
                print('Timeout when try to get element by xpath: ', xpath)
        return element

    def wait_sec(self, flag, delay_sec):
        nrof_times = int(delay_sec * 2)
        for i in range(nrof_times):
            pass

    def parse_data(self, input_data):
        # if self.page_genomes_title not in self.driver.title:
        self.load_page(self.url_genomes, self.win_genomes_idx)
        self.driver.switch_to_window(self.driver.window_handles[self.win_genomes_idx])
        waiter = WebDriverWait(self.driver, 10)
        input_button = self.driver.find_element_by_class_name(self.input_button_class_name)
        input_box = self.get_element(self.input_box_xpath)
        input_box.clear()
        input_box.send_keys(input_data)
        time.sleep(0.5)
        input_button.click()

        loading_var = self.driver.find_element_by_xpath(self.load_variable_xpath)
        while loading_var.is_displayed():
            time.sleep(0.5)

        zoomout_button = self.get_element(self.zoomout_xpath, 5, visible=True, clickable=True)
        zoomout_button.click()

        while loading_var.is_displayed():
            time.sleep(0.5)

        for _ in range(20):
            div_panel = self.get_element(self.loading_div_xpath, 2)
            if div_panel is None:
                continue
            success = waiter.until_not(EC.staleness_of(div_panel))
            if success:
                continue
            if not 'Loading' in div_panel.text:
                break
            time.sleep(0.5)

        for i in range(10):
            page_div = self.get_element(self.page_div_xpath)
            act = ActionChains(self.driver)
            act.context_click(page_div).perform()
            time.sleep(0.5)
            blast_1 = self.get_element(self.blast_select_1_xpath, 2)
            if blast_1 is None:
                time.sleep(0.5)
                continue
            act = ActionChains(self.driver)
            act.move_to_element(blast_1).perform()
            act.send_keys(Keys.ARROW_RIGHT).perform()
            blast_2 = self.get_element(self.blast_select_2_xpath, 2)
            if blast_2 is None:
                time.sleep(0.5)
                continue
            if blast_2.is_displayed():
                act = ActionChains(self.driver)
                act.move_to_element(blast_2)
                time.sleep(0.5)
                blast_2.click()
                time.sleep(3)
                if len(self.driver.window_handles) !=2 :
                    continue
                break

        for i in range(10):
            if len(self.driver.window_handles) == 2:
                break
            time.sleep(0.5)

        if len(self.driver.window_handles) == 1:
            return None
        # waiter = WebDriverWait(self.driver, 10)
        # waiter.until(EC.new_window_is_opened(self.driver.window_handles))

        self.driver.switch_to_window(self.driver.window_handles[1]) ### focus on new page
        text_area = self.get_element(self.text_area_xpath, 3)
        from_box = self.get_element(self.from_box_xpath, 3)
        to_box = self.get_element(self.to_box_xpath, 3)
        if text_area is None or from_box is None or to_box is None:
            self.driver.close()
            return None
        data = [text_area.text, from_box.get_attribute('value'), to_box.get_attribute('value')]
        print('Three data: ', ', '.join(data))
        return data

    def parse_result(self, data):
        if data is None:
            return  'No data'
        assert len(data) == 3, len(data)
        self.load_page(self.url_nucleotide, self.win_nucleotide_idx)
        xpaths = [self.text_area_xpath, self.from_box_xpath, self.to_box_xpath]
        boxes = [self.get_element(x) for x in xpaths]
        blast_button = self.get_element(self.blast_button_xpath)
        for dat, box in zip(data, boxes):
            box.send_keys(dat)
        time.sleep(1)
        database_select = self.get_element(self.database_select_xpath)
        database_select.click()
        time.sleep(1)
        blast_button.click()
        table = self.get_element(self.result_table_xpath, 90, visible=True)
        result_panel = self.get_element(self.reult_panel_xpath, 90, visible=True)
        if table is None:
            self.driver.close()
            return 'No result'

        query_covers = table.find_elements_by_class_name('c5')
        ident_percents = table.find_elements_by_class_name('c7')
        nrof_rows = len(query_covers)
        result = ''
        h_percent = '100%'
        if nrof_rows == 2:
            result = 'empty'
        elif nrof_rows >= 3:
            query, ident = query_covers[2].text, ident_percents[2].text
            if query == h_percent and ident == h_percent:
                result = 'same'
            elif query != h_percent and ident != h_percent:
                result = 'not same'
            else:
                result = ident
        self.driver.close()
        return result

    # def __del__(self):
    #     self.driver.quit()

if __name__ == '__main__':
    import argparse
    gene_li = ['16:70954945', '17:4837117', '22:25024072', '13:25467010', '21:35822270', '6:31996297', '6:32549402', '17:44073866']

    worker = WebWorker()
    for data in gene_li:
        result = worker(data)
        print('input: %s, result: %s'%(data, result))
