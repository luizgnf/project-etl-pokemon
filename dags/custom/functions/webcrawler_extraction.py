import json
import requests
import logging
from bs4 import BeautifulSoup


def cardmarket_pricehistory(sql_dataframe):
   
    pricehistory_array = []
    
    for index, row in sql_dataframe.iterrows():
        
        try:
            # Access URL from sql dataframe
            response = requests.get(row['url'])
            
            # Get chart tag by sequential filters
            soup = BeautifulSoup(response.text, 'html.parser') 
            tag_str = soup.find(id = 'tabContent-info').find(class_ = "chart-init-script").text

            # Slice string from tag and get only chart data
            datachart_str = tag_str[tag_str.find('{"labels":'):tag_str.rfind(',"options":')]
            datachart_dict = json.loads(datachart_str)

            # Final object to be converted in json
            pricehistory_obj = {
                'card_id': row['card_id'],
                'date': datachart_dict['labels'],
                'avgsellprice': datachart_dict['datasets'][0]['data']
            }
            pricehistory_array.append(pricehistory_obj)
            
            if index % 10 == 0:
                print(f'{index} Scrap process still running...')    
        
        except Exception as e:
            print(f"Card {row['card_id']}: {e}")
            continue

    
    return pricehistory_array