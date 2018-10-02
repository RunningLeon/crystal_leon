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

import pandas as pd
from multiprocessing import Process, Manager
import pickle


### pip install openpyxl

URL_genomes = 'https://www.ncbi.nlm.nih.gov/variation/tools/1000genomes/'
URL_nucleotide = 'https://blast.ncbi.nlm.nih.gov/Blast.cgi?PAGE=MegaBlast&PROGRAM=blastn&PAGE_TYPE=BlastSearch&BLAST_SPEC=OGP__9606__9558'

def process_sheet(sheet, share_dict, target_col_name, worker, print_every):
    col_names = list(sheet.columns)
    if not target_col_name in col_names:
        print('WARN: Column name %s not exists in sheet with names= %s'%(target_col_name, col_names))
        return None
    target_col_values = list(sheet[target_col_name])
    result_li = []
    nrof_row = len(target_col_values)
    print('Totally %4d rows '%(nrof_row))
    for i, value in enumerate(target_col_values, start=1):
        value = str(value)
        value_li = value.strip().split()
        value = value_li[0].strip()
        if value in share_dict:
            result = share_dict[value]
            if result == 'unknown':
                result = worker(value)
        else:
            result = worker(value)
        share_dict[value] = result
        print('Update share_dict: key-value= %s : %s'%(value, result))
        if i % print_every == 0:
            print('No. %3d/%3d, input: %s, result: %s'%(i, nrof_row, value, result))
        result_li.append(result)
    return result_li



def process_excel(excel_path, worker, share_dict, output_dir=None, nrof_sheet=3, target_col_name='Chrom:Pos Ref/Alt', print_every=10):
    assert os.path.exists(excel_path), 'File not exists: ' + excel_path
    input_dir, filename = os.path.split(excel_path)
    if output_dir is None:
        output_dir = os.path.join(input_dir, 'output')
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_excel_path = os.path.join(output_dir, filename)
    sheets_dict = pd.read_excel(excel_path, sheet_name=None, header=0)
    sheet_names = list(sheets_dict.keys())
    sheet_chosen_names = sheet_names[:nrof_sheet]
    sheets_output_dict = {name: sheets_dict[name] for name in sheet_names[nrof_sheet:]} #### rest sheet dict
    nrof_sheet_chosen = len(sheet_chosen_names)
    if nrof_sheet_chosen != nrof_sheet:
        print('WARN: Only found %2d sheets in excel: %s' %(len(sheet_names), excel_path))

    for k, sheet_name in enumerate(sheet_chosen_names, start=1):
        sheet = sheets_dict[sheet_name]
        print('Processing no. %2d/%2d sheet: %s' %(k, nrof_sheet_chosen, sheet_name))
        result_li = process_sheet(sheet, share_dict, target_col_name, worker, print_every)
        sheet_new = sheet.copy()
        # 添加到最后一列
        if result_li is not None:
            sheet_new[target_col_name + '_result'] = result_li
        sheets_output_dict.update({sheet_name:sheet_new})
    ### write output excel
    excel_writer = pd.ExcelWriter(output_excel_path)
    try:
        for sheet_name in sheet_names:
            sheet = sheets_output_dict[sheet_name]
            sheet.to_excel(excel_writer, sheet_name)
    except Exception as e:
        print('WARN: error when writing ' + output_excel_path)
        print(e)
    finally:
        excel_writer.close()
        print('Finishing writing ' + output_excel_path)


    
class WebWorker(object):
    
    def __init__(self, url_genomes=URL_genomes, url_nucleotide=URL_nucleotide, executable_path='chromedriver', log_dir='./log', headless=True):
        """
        :param url_genomes: url
        :param url_nucleotide: url
        :param executable_path: chromedriver path, for windows, it should be like: 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chromedriver.exe'

        Usage:
        For windows, first install chrome browser, then check chrome version, then download webdriver. For more detailed instruction,
            pls. check https://blog.csdn.net/u013360850/article/details/54962248
        """
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--lang=en-us')
        chrome_options.add_argument('--no-sandbox') # required when running as root user. otherwise you would get no sandbox errors. 
        if headless:
            chrome_options.add_argument('--headless')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_path = os.path.join(log_dir, 'chromedriver.log')
        driver = webdriver.Chrome(executable_path, chrome_options=chrome_options, 
                service_args=['--verbose', '--log-path=' + log_path])

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
        self.waiting_p_xpath = '//*[@id="content"]/p[2]'


    def __call__(self, input_data):
        result = 'unknown'
        try:
            data = self.parse_data(input_data)
            result = self.parse_result(data)
        except Exception as e:
            print(e)
        finally:
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
                break
        return element


    def parse_data(self, input_data):
        window_handles = self.driver.window_handles
        nrof_win = len(window_handles)
        if nrof_win >= 2:
            for i in range(1, nrof_win):
                self.driver.switch_to_window(window_handles[i])
                self.driver.close()

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

        for _ in range(20):
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
        print('Three data: ', data)

        return data

    def parse_result(self, data):
        if data is None:
            return  'unknown'
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
        table = self.get_element(self.result_table_xpath, 60, visible=True) ### wait no more than 90 sec
        result_panel = self.get_element(self.reult_panel_xpath, 10, visible=True) ### wait no more than 3 sec
        if table is None:
            self.driver.close()
            return 'unknown'

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

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()



if __name__ == '__main__':
    import argparse
    import glob
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-dir', type=str, default='.')
    parser.add_argument('-o', '--output-dir', type=str, default='./output')
    parser.add_argument('-ext', '--extension', type=str, default='xlsx')
    parser.add_argument('-a', '--aug', type=str, choices=['train', 'val'], default='val')
    parser.add_argument('--headless', action='store_false', help='Whether to set headless mode.')
    parser.add_argument('-p', '--pickle-file', type=str, default='./output/keys.pkl')
    parser.add_argument('-exe', '--exe-path', type=str, default='chromedriver', help='For windows, input path of "chromedriver.exe"')
    args = parser.parse_args()
    assert os.path.exists(args.input_dir) and os.path.isdir(args.input_dir), 'Directory not exists: ' + args.input_dir
    keys_dict = None
    pickle_dir = os.path.split(args.pickle_file)[0]
    if not os.path.exists(pickle_dir):
        os.makedirs(pickle_dir)

    try:
        if os.path.exists(args.pickle_file):
            with open(args.pickle_file, 'rb') as f:
                keys_dict = pickle.load(f)
    except Exception as e:
        print(e)
        keys_dict = None

    excel_paths = glob.glob(os.path.join(args.input_dir, '*.' + args.extension))
    nrof_excel = len(excel_paths)
    print('Totally %2d excel found in %s' %(nrof_excel, args.input_dir))
    if nrof_excel:
            with Manager() as manager:
                keys_dict_share = manager.dict()
                if keys_dict is not None:
                    keys_dict_share.update(keys_dict)

                try:
                    excel_paths = sorted(excel_paths)

                    worker_li = [WebWorker(executable_path=args.exe_path, headless=args.headless) for _ in range(nrof_excel)]
                    process_list = []

                    for i, excel in enumerate(excel_paths):
                        func_args = (excel, worker_li[i], keys_dict_share, args.output_dir)
                        p = Process(target=process_excel, args=func_args)
                        process_list.append(p)


                    for p in process_list:
                        p.start()

                    for p in process_list:
                        p.join()
                        
                    print('All done.')
                except Exception as e:
                    print(e)
                else:
                    pass
                finally:
                    with open(args.pickle_file, 'wb') as f:
                        pickle.dump(keys_dict_share, f)
                    print('Finishing update keys to ' + args.pickle_file)
