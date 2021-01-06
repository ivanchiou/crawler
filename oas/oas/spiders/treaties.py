import scrapy
import os
import re
import csv
import time
import datetime
from bs4 import BeautifulSoup
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class CaseInsensitiveDict(dict):
    def __setitem__(self, key, value):
        super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)

    def __getitem__(self, key):
        return super(CaseInsensitiveDict, self).__getitem__(key.lower())

class TreatiesSpider(CrawlSpider):
    name = 'treaties'
    allowed_domains = ['www.oas.org']
    start_urls = ['http://www.oas.org/DIL/treaties_signatories_ratifications_member_states_san_kitts_and_nevis.htm']
    data_list = []
    crawl_col_list = ['Adopted at', 'Date', "Depository", "ENTRY INTO FORCE"]
    fieldnames = ['treaty_title','treaty_done_place','treaty_done_date','treaty_upload_file','treaty_agreement_type','treaty_status','treaty_series_number','treaty_depository','treaty_status_list_site','treaty_subject','treaty_signature_place','treaty_signature_date','treaty_implemention_requirements','treaty_entry_into_force_date','treaty_entry_into_force_conditions','treaty_acceptance','treaty_amendments_and_notes','treaty_last_updated']

    mapping_dict = CaseInsensitiveDict()
    mapping_dict[crawl_col_list[0]] = fieldnames[1]
    mapping_dict[crawl_col_list[1]] = fieldnames[2]
    mapping_dict[crawl_col_list[2]] = fieldnames[7]
    mapping_dict[crawl_col_list[3]] = fieldnames[13]
    existed_dict = {
        fieldnames[0]: '',
        fieldnames[1]: '',
        fieldnames[2]: '',
        fieldnames[3]: None,
        fieldnames[4]: 'Multilateral',
        fieldnames[5]: 'In Force',
        fieldnames[6]: '',
        fieldnames[7]: '',
        fieldnames[8]: 'N/A',
        fieldnames[9]: 'Agreement',
        fieldnames[10]: '',
        fieldnames[11]: '',
        fieldnames[12]: 'N/A',
        fieldnames[13]: '',
        fieldnames[14]: 'N/A',
        fieldnames[15]: 'N/A',
        fieldnames[16]: 'N/A',
        fieldnames[17]: datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    }

    def start_requests(self):
        with open('output.csv', 'w') as csvfile:
            csv.DictWriter(csvfile, fieldnames=self.fieldnames)      
        for url in self.start_urls:
            yield scrapy.Request(url, callback = self.parse_item,
            dont_filter=True)

    print('Continue..')
    def parse_item(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        print('Processing..')
        links = soup.find_all('a')
        for link in links:
            href = link.get('href')
            regex = re.compile(r'[\n\r\t]')
            text = regex.sub("", link.text)
            if href != None and (href.find('http://www.oas.org/juridico/english') != -1 or href.find('http://www.oas.org/en/sla/dil') != -1):
                yield scrapy.Request(href, callback = self.parse_page, 
                dont_filter=True, cb_kwargs=dict(href=href, title=text))
        pass

    def parse_page(self, response, href, title):
        soup = BeautifulSoup(response.text, 'lxml')
        print('Processing Page..')
        item_dict = self.existed_dict.copy()
        series_number_index = None
        try:
            series_number_index = title.rindex("(")+1
            if series_number_index != None:
                item_dict.update({self.fieldnames[0]: title, self.fieldnames[6]: title[series_number_index:len(title)-1]})
                for search_key in self.crawl_col_list:
                    force = soup.findAll(text=re.compile(".*"+search_key+":.*", flags=re.IGNORECASE))
                    if force != None and len(force) != 0:
                        lines = force[0].splitlines()
                        for line in lines:
                            m = re.search(".*"+search_key+":", line, re.IGNORECASE)
                            if m != None:
                                value = ''
                                if line[m.end():] and line[m.end():].strip():
                                    value = line[m.end():].replace(u'\xa0', '')
                                else:
                                    if len(force[0].parent.parent.contents) > 1:
                                        try:
                                            value = force[0].parent.parent.contents[1].replace(u'\xa0', '')
                                        except:
                                            pass
                                    else:
                                        pattern = re.compile(search_key+":", re.IGNORECASE)
                                        try:
                                            value = pattern.sub("", force[0].parent.contents[0]).replace(u'\xa0', '')
                                        except:
                                            value = pattern.sub("", force[0].parent.contents[1]).replace(u'\xa0', '')
                                regex = re.compile(r'[\n\r\t]')
                                value = regex.sub("", value)
                                if search_key == 'Adopted at':
                                    item_dict['treaty_signature_place'] = value
                                elif search_key == 'Date' or search_key == 'ENTRY INTO FORCE':
                                    if search_key == 'ENTRY INTO FORCE':
                                        value = value.lstrip()
                                        value_list = value.split(' ')
                                        value = value_list[0]
                                        value = value.replace(",", "")
                                    value = value.replace(" ", "")
                                    try:
                                        dateObj = datetime.datetime.strptime(value, "%m/%d/%y").date()
                                        value = dateObj.strftime('%Y-%m-%d')
                                    except:
                                        try:
                                            dateObj = datetime.datetime.strptime(value, "%m/%d/%Y").date()
                                            value = dateObj.strftime('%Y-%m-%d')
                                        except:
                                            if search_key == 'ENTRY INTO FORCE':
                                                value = item_dict['treaty_done_date']        
                                    finally:
                                        if search_key == 'Date':
                                            item_dict['treaty_signature_date'] = value
                                            item_dict['treaty_entry_into_force_date'] = value
                                item_dict[self.mapping_dict[search_key]] = value
        except:
            pass
                

        force = soup.find(text=re.compile(".*"+"window.location=", flags=re.IGNORECASE))
        if force != None and len(force) != 0:
            force_list = force.split('"')
            href = force_list[1]
            yield scrapy.Request(href, callback = self.parse_page, dont_filter=True, cb_kwargs=dict(href=href, title=title))
        elif len(item_dict) > 0 and item_dict['treaty_series_number'] !='':
                #print(item_dict)
                self.data_list.append(item_dict.copy())
                with open('output.csv', 'a', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                    writer.writerow(item_dict.copy())