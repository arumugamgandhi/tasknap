import gdown
import os
import pandas as pd
import numpy as np
import json
from flask import Flask, jsonify, request
app = Flask(__name__)
#url = 'https://drive.google.com/a/greendeck.co/uc?id=19r_vn0vuvHpE-rJpFHvXHlMvxa8UOeom&export=download'
#def init_files(dump_path = 'dumps/netaporter_gb.json'):
#    if dump_path.split('/')[0] not in os.listdir():
#        os.mkdir(dump_path.split('/')[0])
#    if os.path.exists(dump_path):
#        pass
#    else:
#        gdown.download(url = url, output = dump_path, quiet=False)

#init_files('dumps/netaporter_gb.json')
#global json_data,Products_data
json_data = pd.DataFrame()# To store the given json_data as DF
Products_data = pd.DataFrame()# To new features added to json_data DF (Discounts, Brand_name)
# Discount is calculated as regular price - offer price
# Discount percentage is calculated by (Discount/regular price)*100 and added as a new feature in DF
def prepare_dataset(path = 'netaporter_gb_similar.json'):
    offer_price_list = []
    regular_price_list = []
    Discount_percentage = []
    brands_list = []
    global json_data
    global Products_data
    product_json=[]
    with open(path) as fp:
        for product in fp.readlines():
            product_json.append(json.loads(product))
    json_data=pd.DataFrame(product_json)
    for products in range(0,len(json_data)):
        price_dict = json_data['price'].values[products]
        dump = json.dumps(price_dict)
        price_json_obj = json.loads(dump)
        get_offer_price = price_json_obj.get('offer_price')
        offer_price_list.append(get_offer_price.get('value'))
        get_regular_price = price_json_obj.get('regular_price')
        regular_price_list.append(get_regular_price.get('value'))
        Discount = get_regular_price.get('value') - get_offer_price.get('value')
        rounded_percentage = round((Discount/get_regular_price.get('value'))*100,2)
        Discount_percentage.append(rounded_percentage)
        brand_dict = json_data['brand'].values[products]
        brand_dump = json.dumps(brand_dict)
        brand_json_obj = json.loads(brand_dump)
        get_brand_name = brand_json_obj.get('name')
        brands_list.append(get_brand_name)
    Products_data = json_data.assign(Discounts = Discount_percentage,Brand_name = brands_list)
   
prepare_dataset('netaporter_gb_similar.json')

#To perform first sub_task
def nap_product_discount(inp_data,req_query,operator,operand1,operand2):
    Product_id_list = []
    if operand1 == "discount":
        if operator == ">":
            result_df = inp_data[inp_data.Discounts>operand2]
        elif operator == "<":
            result_df = inp_data[inp_data.Discounts<operand2]
        elif operator == "==":
            result_df = inp_data[inp_data.Discounts==operand2]
        else:
            output = [{'Operator Error' : "Please POST a valid operator"}]
            return output
        for products_id in range(0,len(result_df)):
            results_dict = result_df['_id'].values[products_id]
            results_dump = json.dumps(results_dict)
            results_json_obj = json.loads(results_dump)
            get_products_id = results_json_obj.get('$oid')
            Product_id_list.append(get_products_id)
        return Product_id_list
    elif operand1 == "brand.name":
        brand_spec_data = inp_data[inp_data.Brand_name==operand2]
        for products_id in range(0,len(brand_spec_data)):
            results_dict = brand_spec_data['_id'].values[products_id]
            results_dump = json.dumps(results_dict)
            results_json_obj = json.loads(results_dump)
            get_products_id = results_json_obj.get('$oid')
            Product_id_list.append(get_products_id)
        return Product_id_list
    else:
        output = [{'Operand Error' : "Please POST a valid operand1"}]
        return output
#To perform second sub_task
def Brand_avg_discount(inp_data,req_query,operator,operand1,operand2):
    Response_dict = {}
    if operand1 == "brand.name":
        brand_df = inp_data[inp_data.Brand_name==operand2]
        count = len(brand_df)
        total_discount = brand_df['Discounts'].sum()
        avg = total_discount/count
        avg_dis = round(avg,2)
        Response_dict['avg_discount']=avg_dis
        Response_dict['discounted_products_count']=count
        return Response_dict
    elif operand1 == "discount":
        if operator == ">":
            result_df = inp_data[inp_data.Discounts>operand2]
        elif operator == "<":
            result_df = inp_data[inp_data.Discounts<operand2]
        elif operator == "==":
            result_df = inp_data[inp_data.Discounts==operand2]
        else:
            output = {'Operator Error' : "Please POST a valid operator"}
            return output
        count = len(result_df)
        total_discount = result_df['Discounts'].sum()
        avg = total_discount/count
        avg_dis = round(avg,2)
        Response_dict['avg_discount']=avg_dis
        Response_dict['discounted_products_count']=count
        return Response_dict
    else:
        output = {'Operand Error' : "Please POST a valid operand1"}
        return output
#To perform third sub_task
def ex_list(json_data,operand1="none",operand2="none"):
    expensive = []
    if operand1 == "none":
      for prod in range(0,len(json_data)):
        similar_products_value = json_data['similar_products'].values[prod]
        if(type(similar_products_value)==type(2.4)):
          continue
        value_dump = json.dumps(similar_products_value)
        similar_products_json_obj = json.loads(value_dump)
        get_meta = similar_products_json_obj.get('meta')
        get_total_results= get_meta.get("total_results")
        if(get_total_results==0):
          continue
        prod_price_dict = json_data['price'].values[prod]
        dump_price = json.dumps(prod_price_dict)
        price_json_obj_v = json.loads(dump_price)
        get_basket_price = price_json_obj_v.get('basket_price')
        NAP_basket_price = get_basket_price.get('value')
        web_results = similar_products_json_obj.get('website_results')
        competitor= web_results.get("5d0cc7b68a66a100014acdb0")
        knn_list= competitor.get("knn_items")
        if(len(knn_list)==1):
          competitor_source_json= knn_list[0]["_source"]
          competitor_price_json = competitor_source_json.get("price")
          competitor_basket_price_json = competitor_price_json.get("basket_price")
          competitor_basket_price_value = competitor_basket_price_json.get("value")
          if(NAP_basket_price>competitor_basket_price_value):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        competitor= web_results.get("5da94e940ffeca000172b12a")
        knn_list = competitor.get("knn_items")
        if(len(knn_list)==1):
          competitor_source_json= knn_list[0]["_source"]
          competitor_price_json = competitor_source_json.get("price")
          competitor_basket_price_json = competitor_price_json.get("basket_price")
          competitor_basket_price_value = competitor_basket_price_json.get("value")
          if(NAP_basket_price>competitor_basket_price_value):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        competitor= web_results.get("5da94ef80ffeca000172b12c")
        knn_list = competitor.get("knn_items")
        if(len(knn_list)==1):
          competitor_source_json= knn_list[0]["_source"]
          competitor_price_json = competitor_source_json.get("price")
          competitor_basket_price_json = competitor_price_json.get("basket_price")
          competitor_basket_price_value = competitor_basket_price_json.get("value")
          if(NAP_basket_price>competitor_basket_price_value):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        competitor= web_results.get("5da94f270ffeca000172b12e")
        knn_list = competitor.get("knn_items")
        if(len(knn_list)==1):
          competitor_source_json= knn_list[0]["_source"]
          competitor_price_json = competitor_source_json.get("price")
          competitor_basket_price_json = competitor_price_json.get("basket_price")
          competitor_basket_price_value = competitor_basket_price_json.get("value")
          if(NAP_basket_price>competitor_basket_price_value):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        competitor= web_results.get("5da94f4e6d97010001f81d72")
        knn_list = competitor.get("knn_items")
        if(len(knn_list)==1):
          competitor_source_json= knn_list[0]["_source"]
          competitor_price_json = competitor_source_json.get("price")
          competitor_basket_price_json = competitor_price_json.get("basket_price")
          competitor_basket_price_value = competitor_basket_price_json.get("value")
          if(NAP_basket_price>competitor_basket_price_value):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
    elif operand1 == "brand.name":
        spec_data = json_data[json_data.Brand_name==operand2]
        for prod in range(0,len(spec_data)):
            similar_products_value = spec_data['similar_products'].values[prod]
            if(type(similar_products_value)==type(2.4)):
              continue
            value_dump = json.dumps(similar_products_value)
            similar_products_json_obj = json.loads(value_dump)
            get_meta = similar_products_json_obj.get('meta')
            get_total_results= get_meta.get("total_results")
            if(get_total_results==0):
              continue
            prod_price_dict = spec_data['price'].values[prod]
            dump_price = json.dumps(prod_price_dict)
            price_json_obj_v = json.loads(dump_price)
            get_basket_price = price_json_obj_v.get('basket_price')
            NAP_basket_price = get_basket_price.get('value')
            web_results = similar_products_json_obj.get('website_results')
            competitor= web_results.get("5d0cc7b68a66a100014acdb0")
            knn_list = competitor.get("knn_items")
            if(len(knn_list)==1):
              competitor_source_json= knn_list[0]["_source"]
              competitor_price_json = competitor_source_json.get("price")
              competitor_basket_price_json = competitor_price_json.get("basket_price")
              competitor_basket_price_value = competitor_basket_price_json.get("value")
              if(NAP_basket_price>competitor_basket_price_value):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            competitor= web_results.get("5da94e940ffeca000172b12a")
            knn_list = competitor.get("knn_items")
            if(len(knn_list)==1):
              competitor_source_json= knn_list[0]["_source"]
              competitor_price_json = competitor_source_json.get("price")
              competitor_basket_price_json = competitor_price_json.get("basket_price")
              competitor_basket_price_value = competitor_basket_price_json.get("value")
              if(NAP_basket_price>competitor_basket_price_value):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            competitor= web_results.get("5da94ef80ffeca000172b12c")
            knn_list = competitor.get("knn_items")
            if(len(knn_list)==1):
              competitor_source_json= knn_list[0]["_source"]
              competitor_price_json = competitor_source_json.get("price")
              competitor_basket_price_json = competitor_price_json.get("basket_price")
              competitor_basket_price_value = competitor_basket_price_json.get("value")
              if(NAP_basket_price>competitor_basket_price_value):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            competitor= web_results.get("5da94f270ffeca000172b12e")
            knn_list = competitor.get("knn_items")
            if(len(knn_list)==1):
              competitor_source_json= knn_list[0]["_source"]
              competitor_price_json = competitor_source_json.get("price")
              competitor_basket_price_json = competitor_price_json.get("basket_price")
              competitor_basket_price_value = competitor_basket_price_json.get("value")
              if(NAP_basket_price>competitor_basket_price_value):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            competitor= web_results.get("5da94f4e6d97010001f81d72")
            knn_list = competitor.get("knn_items")
            if(len(knn_list)==1):
              competitor_source_json= knn_list[0]["_source"]
              competitor_price_json = competitor_source_json.get("price")
              competitor_basket_price_json = competitor_price_json.get("basket_price")
              competitor_basket_price_value = competitor_basket_price_json.get("value")
              if(NAP_basket_price>competitor_basket_price_value):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue 
    else:
        output = [{'Operand Error' : "Please POST web_results valid operand1"}]
        return output    
    return expensive
#To perform forth sub_task
def higher_price_list(json_data,competitor_id,operand2):
  higher = []
  for prod in range(0,len(json_data)):
    similar_products_value = json_data['similar_products'].values[prod]
    if(type(similar_products_value)==type(2.4)):
      continue
    value_dump = json.dumps(similar_products_value)
    similar_products_json_obj = json.loads(value_dump)
    get_meta = similar_products_json_obj.get('meta')
    get_total_results= get_meta.get("total_results")
    if(get_total_results==0):
      continue
    prod_price_dict = json_data['price'].values[prod]
    dump_price = json.dumps(prod_price_dict)
    price_json_obj_v = json.loads(dump_price)
    get_nap_offer_price = price_json_obj_v.get('basket_price')
    NAP_offer_price = get_nap_offer_price.get('value')
    web_results = similar_products_json_obj.get('website_results')
    if competitor_id == "5d0cc7b68a66a100014acdb0":
        competitor= web_results.get("5d0cc7b68a66a100014acdb0")
    elif competitor_id == "5da94e940ffeca000172b12a":
        competitor= web_results.get("5da94e940ffeca000172b12a")
    elif competitor_id == "5da94ef80ffeca000172b12c":
        competitor= web_results.get("5da94ef80ffeca000172b12c")
    elif competitor_id == "5da94f270ffeca000172b12e":
        competitor= web_results.get("5da94f270ffeca000172b12e")
    elif competitor_id == "5da94f4e6d97010001f81d72":
        competitor= web_results.get("5da94f4e6d97010001f81d72")
    knn_list = competitor.get("knn_items")
    if(len(knn_list)==1):
      competitor_source_json= knn_list[0]["_source"]
      competitor_price_json = competitor_source_json.get("price")
      competitor_basket_price_json = competitor_price_json.get("basket_price")
      competitor_basket_price_value = competitor_basket_price_json.get("value")
      n_percent_of_comp_price = competitor_basket_price_value/operand2
      if(NAP_offer_price>competitor_basket_price_value+n_percent_of_comp_price):
        p_id = json_data['_id'].values[prod]
        higher.append(p_id.get("$oid"))
  return higher
#To obtain filters from requested json_query
def get_filter(query,request_json_obj):
    try:
        if query == "competition_discount_diff_list":
            filters = request_json_obj.get('filters')
            op11=filters[0]['operand1']
            op1=filters[0]['operator']
            op21=filters[0]['operand2']
            op12=filters[1]['operand1']
            op2=filters[1]['operator']
            op22=filters[1]['operand2']
            return filters,op11,op1,op21,op12,op2,op22
        else:
            filters = request_json_obj.get('filters')
            op1=filters[0]['operand1']
            op=filters[0]['operator']
            op2=filters[0]['operand2']
            return filters,op1,op2,op
    except:
        output = {'Operator Error' : "Please POST a valid operator"}
        return output
        
    
@app.route('/', methods=['POST'])

def Process_inp():
    #request = { "query_type": "discounted_products_list", "filters": [{ "operand1": "discount", "operator": "==", "operand2": 69.93 }] }
    data = request.get_json(force=True)
    request_data = json.dumps(data)
    request_data_obj = json.loads(request_data)
    query = request_data_obj.get('query_type')
    if query == "discounted_products_list":
        filters,op1,op2,op = get_filter(query,request_data_obj)
        l_=nap_product_discount(Products_data,query,op,op1,op2)
        output = {'discounted_products_list': l_}
        return jsonify(output)
    elif query == "discounted_products_count|avg_discount":
        filters,op1,op2,op = get_filter(query,request_data_obj)
        output=Brand_avg_discount(Products_data,query,op,op1,op2)
        return jsonify(output)
    elif query == "expensive_list":
        filters = request_data_obj.get('filters')
        if (filters):
            filters,op1,op2,op = get_filter(query,request_data_obj)
            exp_lst = ex_list(Products_data,op1,op2)
            output = {'expensive_list': exp_lst}
            return jsonify(output)
        else:
            exp_lst = ex_list(Products_data)
            output = {'expensive_list': exp_lst}
            return jsonify(output)
    elif query == "competition_discount_diff_list":
        filters,op11,op1,op21,op12,op2,op22 = get_filter(query,request_data_obj)
        hig_lst = higher_price_list(Products_data,op22,op21)
        output = {'competition_discount_diff_list': hig_lst}
        return jsonify(output)
    else:
        output = {'Query Error' : "Please POST a valid query"}
        return jsonify(output)
        
#prepare_dataset('netaporter_gb_similar.json')

if __name__ == '__main__':
    app.run(port = 5000, debug=True)




