gene_li = ['16:70954945', '17:4837117', '22:25024072']
driver = webdriver.Chrome()
driver.get(url)
input_, div = [driver.find_element_by_id(x) for x in [id_input, id_div]]
button_input = driver.find_element_by_class_name(clsname_btn)

input_.send_keys(gene_li[1])
button_input.click()
time.sleep(2)
button_zoomout = driver.find_element_by_xpath("//a[@class='x-btn x-unselectable x-box-item x-toolbar-item x-btn-default-toolbar-small'][5]")
button_zoomout.click()
time.sleep(3)
act = ActionChains(driver)

act = act.context_click(div)
act = act.send_keys(Keys.ARROW_DOWN)
act = act.perform()
time.sleep(3)
blast_1 = driver.find_element_by_xpath('//*[contains(text(), "BLAST and Primer Search")]')
act_2 = ActionChains(driver)
act_2.move_to_element(blast_1).perform()
time.sleep(2)
blast_2 = driver.find_element_by_xpath('//*[contains(text(), "BLAST Search (Visible Range)")]')
blast_2.click()
time.sleep(2)
nrof_win = len(driver.window_handles)
assert nrof_win >=2, 'windows number %s'%nrof_win
win_home = driver.window_handles[0]
win_data = driver.window_handles[1]
driver.switch_to_window(win_data)
text_area = driver.find_element_by_xpath('//*[@id="seq"]')
from_box = driver.find_element_by_xpath('//*[@id="QUERY_FROM"]')
to_box = driver.find_element_by_xpath('//*[@id="QUERY_TO"]')
data = [text_area.text, from_box.get_attribute('value'), to_box.get_attribute('value')]
print(data)

url_2 = 'https://blast.ncbi.nlm.nih.gov/Blast.cgi?PAGE=MegaBlast&PROGRAM=blastn&PAGE_TYPE=BlastSearch&BLAST_SPEC=OGP__9606__9558'
driver.get(url_2)
time.sleep(1)
# database_select = driver.find_element_by_xpath('//*[@id="DATABASE"]')
# database_select.click()
# time.sleep(0.2)
target_select = driver.find_element_by_xpath('//*[@id="DATABASE"]/option[14]')
target_select.click()
# target_select.send_keys(Keys.ENTER)
text_area = driver.find_element_by_xpath('//*[@id="seq"]')
from_box = driver.find_element_by_xpath('//*[@id="QUERY_FROM"]')
to_box = driver.find_element_by_xpath('//*[@id="QUERY_TO"]')

text_area.send_keys(data[0])
from_box.send_keys(data[1])
to_box.send_keys(data[2])

blast_button = driver.find_element_by_xpath('//*[@id="b1"]')
blast_button.click()
time.sleep(5)
table = driver.find_element_by_xpath('//*[@id="dscTable"]') 
query_covers = table.find_elements_by_class_name('c5')
idents = table.find_elements_by_class_name('c7')


