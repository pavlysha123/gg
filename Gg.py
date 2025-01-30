import configparser, gspread, sys, os, time, datetime, math, json
from loguru import logger
from seleniumbase import SB
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from urllib.parse import urlparse

sys.argv.append("-n")

script_directory = os.path.dirname(os.path.realpath(__file__))

"Дату из Авито форматируем в нужный формат"
def format_date(date: str) -> datetime.date:
       if 'сегодня' in date:
              return datetime.datetime.now().strftime('%d.%m.%Y')

       if 'вчера' in date:
              return (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d.%m.%Y')

       month_arr = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
       for key, month in enumerate(month_arr):
              if month in date:
                     date = date.replace(month,str(key + 1))
                     newDate = datetime.datetime.strptime(date, '%d %m в %H:%M')
                     year = datetime.datetime.now().year
                     if newDate.year != 1900:
                            year -= newDate.year
                     return datetime.datetime.strptime(f"{newDate.day}.{newDate.month}.{year}", '%d.%m.%Y').strftime('%d.%m.%Y')  

"Удаляем все, кроме цифр и точки"
def remove_non_numeric(input_string):
       return ''.join(char for char in input_string if char in set('0123456789.'))

def get_column_name(index):
       if index < 26:
              return chr(65 + index)
       else:
              first_letter = chr(65 + (index // 26) - 1)
              second_letter = chr(65 + (index % 26))
              return f"{first_letter}{second_letter}"

"Парсинг товаров на avito.ru"
class AvitoParse:
       "Получаем и устанавливаем значения"
       def __init__(self,
                     url: str,
                     success: str,
                     fail: str,
                     name_list: str,
                     spreadsheetId: str,
                     max_threads: int,
                     debug_mode: int = 0,
              ):
              self.url = url
              self.spreadsheetId = spreadsheetId
              self.max_threads = max_threads
              self.name_list = name_list
              self.fail = fail
              self.success = success
              self.line_sheet = 0
              self.data = []
              self.batch_update = []
              self.viewed_list = []
              self.debug_mode = debug_mode
              self.connect = self.connect_google_sheets()
              self.domain = self.get_domain()
              self.array_save_sheet = {}
              self.settings_sheet()
       
       def get_domain(self):
              parsed_url = urlparse(self.url)
              return parsed_url.scheme + "://" + parsed_url.netloc

       "Подключение к Google Таблице по Credentials и Spreadsheet"
       def connect_google_sheets(self):
              creds = Credentials.from_service_account_file(os.path.join(script_directory, 'credentials.json'), scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'])
              gc = gspread.authorize(creds)
              return gc.open_by_key(self.spreadsheetId)

       def creation_list(self):
              name_list = f'{self.name_list}{self.array_save_sheet['count']}'
              existing_sheets = [sheet.title for sheet in self.connect.worksheets()]

              while name_list in existing_sheets:
                     name_list = f"{self.name_list}{self.array_save_sheet['count']}"
                     self.array_save_sheet['count'] += 1
              self.sheet = self.connect.add_worksheet(name_list, 10000, len(self.sheet_headers))
              batch_update_new_list = [{'range': f'{self.sheet.title}!{get_column_name(i)}1:{get_column_name(i)}','values': [[value]]} for i, value in enumerate(self.sheet_headers)]
              self.sheet.spreadsheet.values_batch_update({'value_input_option': 'USER_ENTERED','data': batch_update_new_list})
              return name_list

       "Настройки по таблице"
       def settings_sheet(self):
              self.sheet = False
              self.sheet_headers = self.connect.worksheet('Названия столбцов').row_values(1)
              save_sheet_list = os.path.join(script_directory, 'save_sheet_list.json')
              
              if os.path.isfile(save_sheet_list):
                     with open(save_sheet_list, 'r') as file:
                            try:
                                   self.array_save_sheet = json.load(file)
                            except json.decoder.JSONDecodeError:
                                   self.array_save_sheet = {}
              else:
                     with open(save_sheet_list, 'w') as file:
                            self.array_save_sheet = {}

              if 'lists' in self.array_save_sheet and 'count' in self.array_save_sheet:
                     for lists in self.array_save_sheet['lists']:
                            if 'url' in lists and lists['url'] == self.url and 'name' in lists:
                                   try:
                                          self.sheet = self.connect.worksheet(lists['name'])
                                   except WorksheetNotFound:
                                          self.sheet = False
              if not self.sheet:
                     self.array_save_sheet['lists'] = self.array_save_sheet['lists'] if 'lists' in self.array_save_sheet else []
                     self.array_save_sheet['count'] = self.array_save_sheet['count'] if 'count' in self.array_save_sheet else 1

                     name_list = self.creation_list()
                     logger.info(f"Создали новый лист {name_list}")
                
                     change = False
                     for index, lists in enumerate(self.array_save_sheet['lists']):
                            if 'url' in lists and lists['url'] == self.url:
                                   self.array_save_sheet['lists'][index]['name'] = name_list
                                   change = True
                     if not change:
                            self.array_save_sheet['lists'].append({'url': self.url,'name': name_list})
                     with open(save_sheet_list, 'w') as file:
                            json.dump(self.array_save_sheet, file)
              if self.sheet:
                     self.all_link_sheet = self.all_link_values_sheet()
                     self.sheet_get_all_records = self.sheet.get_all_records()
                     self.sheet_title = self.sheet.title

       "Получение данных с столбца 'Ссылка'"
       def all_link_values_sheet(self) -> dict:
              link_name = 'Ссылка'
              if cell_list := self.sheet.findall(link_name):
                     column_data = self.sheet.col_values(cell_list[0].col)[1:]
              return column_data

       "Добавление и обновление"
       def update_or_add_row_sheets(self, data: dict) -> dict:
              link_name = 'Ссылка'
              days_active = 'Дней активно'

              "Проверяет, существует ли ссылка на листе"
              cell_list = None
              if data[link_name] in self.all_link_sheet:
                     cell_list = self.all_link_sheet.index(data[link_name])

              if cell_list is not None or cell_list == 0:
                     "Обновляем существующию строку (ищется по столбцу 'Ссылка')"
                     row_data = list(self.sheet_get_all_records[cell_list].values())
                     update_data = row_data.copy()

                     "Если нет мест, то добавляем места для значений"
                     if len(row_data) < len(self.sheet_headers):
                            for i in range(len(self.sheet_headers) - len(row_data)):
                                   update_data[i] = ''

                     "Обновляем только те столбцы, которые были получены в data"
                     for i in range(len(self.sheet_headers)):
                            header = self.sheet_headers[i]
                            if days_active == header and row_data[i]:
                                   update_data[i] = int(row_data[i]) + 1
                            if header in data and update_data[i] != data[header]:
                                   update_data[i] = data[header]

                     "Добавляем обновленные данные в партию"
                     if update_data != row_data:
                            self.batch_update.append({
                                   'range': f'{self.sheet_title}!A{cell_list + 2}',
                                   'values': [update_data]
                            })
              else:
                     "Добавлям новую строку"
                     new_row = [data.get(header, '') for header in self.sheet_headers]
                     self.batch_update.append({
                            'range': f'{self.sheet_title}!A{len(self.sheet_get_all_records) + 2 + self.line_sheet}',
                            'values': [new_row]
                     })
                     "Добавляем +1 строку, так как работаем со старыми данными"
                     self.line_sheet += 1

              "Добавляем/обновляем значения каждые 500 позиций"
              if self.batch_update and len(self.batch_update) >= 500:
                     time.sleep(3)
                     batch_update = self.batch_update
                     self.batch_update = []
                     self.sheet.spreadsheet.values_batch_update({
                            'value_input_option': 'USER_ENTERED',
                            'data': batch_update
                     })

       "Проверяет на уникальность ссылки в базе"
       def is_viewed(self, url: str) -> bool:
              if url in self.viewed_list:
                     return True
              return False

       "Открываем страницу"
       def get_url(self, driver, url, max_retries = 1):
              while max_retries < 5:
                     try:
                            driver.open(url)
                            if "Доступ ограничен" in driver.get_title():
                                   self.get_url(driver, url)
                                   max_retries += 1
                            return
                     except Exception as error:
                            #logger.error(f"Ошибка: {error} (Попытка: {max_retries}). Цикл")
                            max_retries += 1
              logger.debug(f"Не удалось добиться результата после {max_retries} попыток: {url}")

       "Получаем HTML данные страницы"
       def get_html(self, url):
              with SB(uc=True,
                     headed=True if self.debug_mode else False,
                     headless=True if not self.debug_mode else False,
                     page_load_strategy="eager",
                     #proxy="",
                     block_images=True,
                     skip_js_waits=True,
              ) as driver:
                     self.get_url(driver, url)
                     time.sleep(2)
                     if html := driver.get_page_source():
                            return html.encode('UTF-8')
                     return '<html></html>'

       def __parse_full_page(self, data: dict) -> dict:
              if 'Ссылка' not in data:
                     logger.debug("Ссылка не найдена")
                     return data

              url = data.get('Ссылка')
              try:
                     soup = BeautifulSoup(self.get_html(url), 'lxml')
                     if soup.find(attrs={"data-marker": "item-view/closed-warning"}):
                            data['Статус'] = self.fail
                            return data
                     else:
                            data['Статус'] = self.success

                     if title := soup.find(attrs={"data-marker": "item-view/title-info"}):
                            data['Название'] = title.text

                     if description := soup.find(attrs={"data-marker": "item-view/item-description"}):
                            data['Описание'] = description.text.replace('\n', '').replace('\r', '')

                     if price := soup.find(attrs={"property": "product:price:amount"}):
                            data['Цена'] = int(price.get('content'))

                     if address := soup.find(class_='style-item-address__string-wt61A'):
                            data['Адрес'] = address.text

                     if total_views := soup.find(attrs={"data-marker": "item-view/total-views"}):
                            data['Просмотры'] = int(total_views.text.split()[0])

                     if date_public := soup.find(attrs={"data-marker": "item-view/item-date"}):
                            date_public_text = date_public.text.lstrip().rstrip().strip()
                            if '· ' in date_public_text:
                                   date_public_text = date_public_text.replace("· ", '')
                            data['Опубликовано'] = format_date(date_public_text)
                     
                     if seller_name := soup.find(attrs={"property": "vk:seller_name"}):
                            data['Продавец'] = seller_name.get('content')
                     
                     "Параметры, например 'О помещении', 'О здании', 'Характеристики' (Объявление)"
                     if params := soup.select('[data-marker="item-view/item-params"]'):
                            for room in params:
                                   if arrayRoom := room.select('ul li'):
                                          for params in arrayRoom:
                                                 if (text := params.text) and (span_with_class := params.find_all('span', class_=True)) and (elementKey := span_with_class[0]):
                                                        key = elementKey.text.replace(':','').lstrip().rstrip()
                                                        value = text.replace(elementKey.text, '').strip().capitalize()
                                                        data[key] = float(remove_non_numeric(value)) if '.' in value else int(remove_non_numeric(value)) if any(char.isdigit() for char in value) else value

                     if 'Цена' in data and 'Общая площадь' in data:
                            data['Цена за м2'] = int(data['Цена'] / data['Общая площадь'])

                     if 'Этаж' in data:
                            data['Этаж'] = f"'{data['Этаж']}'"

                     data['Загружено'] = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
                     if 'Опубликовано' in data and data['Опубликовано']:
                            data['Дата завершения'] = (datetime.datetime.strptime(data['Опубликовано'], '%d.%m.%Y') + datetime.timedelta(days=30)).strftime('%d.%m.%Y')
                            data['Дней активно'] = int(math.fabs((datetime.datetime.strptime(data['Опубликовано'], '%d.%m.%Y') - datetime.datetime.now()).days))
                     if 'Название' in data:
                            logger.info(f"Получены данные объявления: {data['Название']}")

                     return data
              except Exception as error:
                     logger.error(f"Ошибка: {error}")
                     return False

       def __parse_page(self, page):
              try:
                     soup = BeautifulSoup(self.get_html(f"{self.url}&p={page}"), 'lxml')
                     offers = soup.select('[data-marker="item"]')
                     if offers is None or not offers:
                            logger.debug(f"Не нашел товаров на странице {page}")
                            return True
                     for offer in offers:
                            data = {}
                            if href := offer.find(attrs={"itemprop": "url"}):
                                   url = f"{self.domain}{href.get('href')}" if self.domain not in href.get('href') else href.get('href')
                                   if self.is_viewed(url):
                                          continue
                                   self.viewed_list.append(url)
                                   data['Ссылка'] = url
                            if sub_elements := offer.select('.geo-root-zPwRk p'):
                                   data['Ближайшее метро'] = sub_elements[1].select('span:nth-child(2)')[0].text if len(sub_elements) >= 2 and sub_elements[1].select('span:nth-child(2)') else ''
                                   data['Расстояние до метро'] = sub_elements[1].find(class_='geo-periodSection-bQIE4').text if len(sub_elements) >= 2 and sub_elements[1].find(class_='geo-periodSection-bQIE4') else ''
                            self.data.append(data)
                     logger.info(f"Собрали данные со страницы {page} из {self.max_page}")
              except Exception as error:
                     logger.error(f"Ошибка: {error}")
                     return False

       "Начальная функция"
       def parse(self):
              soupAllPages = BeautifulSoup(self.get_html(self.url), "lxml")

              page = 1
              self.max_page = 10

              if lastPage := soupAllPages.find(class_='styles-module-listItem_last-nHQtS'):
                     self.max_page = int(lastPage.text)

              with ThreadPoolExecutor(max_workers=int(self.max_threads)) as executor:
                     r = {executor.submit(self.__parse_page, page_num): page_num for page_num in range(page, self.max_page + 1)}

              logger.info('Собраны максимальные ссылки с каталога')
              if self.data:
                     logger.info(f"Найдено товаров: {len(self.data)}")
                     if self.all_link_sheet:
                            logger.info("Добавляю в основную базу уникальные ссылки из таблицы")
                            for url in self.all_link_sheet:
                                   if self.is_viewed(url):
                                          continue
                                   self.viewed_list.append(url)
                                   self.data.append({'Ссылка': url})
                            logger.info(f"После добавления уникальных объявлений стало: {len(self.data)}")

                     logger.info("Начал многопоточный перебор ссылок")
                     with ThreadPoolExecutor(max_workers=int(self.max_threads)) as executor:
                            results = executor.map(self.__parse_full_page, self.data)
                            for result in results:
                                   if result:
                                          self.update_or_add_row_sheets(result)
                     
                     "Дозагружаем оставшиеся данные"
                     if self.batch_update:
                            self.sheet.spreadsheet.values_batch_update({
                                   'value_input_option': 'USER_ENTERED',
                                   'data': self.batch_update
                            })
              logger.success('Парсер закончил работу')

if __name__ == "__main__":
       config = configparser.RawConfigParser()
       config.read(os.path.join(script_directory, 'settings.ini'), encoding="utf-8")

       urls = config["Avito"]["URLS"].replace(' ','').split(';')

       for url in urls:
              success = False
              while not success:
                     try:
                            AvitoParse(
                                   url=url,
                                   spreadsheetId=config["Avito"]["SPREADSHEETID"],
                                   max_threads=config["Avito"]["MAX_THREADS"] or 1,
                                   name_list=config["Avito"]["NAME_LIST"],
                                   fail=config["Avito"]["FAIL"],
                                   success=config["Avito"]["SUCCESS"],
                            ).parse()
                            success = True
                     except Exception as error:
                            logger.error(error)
                            success = False
                            logger.error("Ошибка выполнения скрипта. Повторный запуск через 5 секунд...")
                            time.sleep(5)
