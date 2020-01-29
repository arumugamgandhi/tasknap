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
json_data = pd.DataFrame()
Products_data = pd.DataFrame()

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

def nap_product_discount(inp_data,req_query,operator,operand1,operand2):
  if operand1 == "discount":
    if operator == ">":
      result_df = inp_data[inp_data.Discounts>operand2]
    elif operator == "<":
      result_df = inp_data[inp_data.Discounts<operand2]
    elif operator == "==":
      result_df = inp_data[inp_data.Discounts==operand2]
  Product_id_list = []
  for products_id in range(0,len(result_df)):
    results_dict = result_df['_id'].values[products_id]
    results_dump = json.dumps(results_dict)
    results_json_obj = json.loads(results_dump)
    get_products_id = results_json_obj.get('$oid')
    Product_id_list.append(get_products_id)
  return Product_id_list
#Adding brand name to df
def Brand_avg_discount(inp_data,req_query,operator,operand1,operand2):
  Response_dict = {}
  brand_df = inp_data[inp_data.Brand_name==operand2]
  count = len(brand_df)
  total_discount = brand_df['Discounts'].sum()
  avg = total_discount/count
  avg_dis = round(avg,2)
  Response_dict['avg_discount']=avg_dis
  Response_dict['discounted_products_count']=count
  return Response_dict

def ex_list(json_data,operand2="none"):
    l1_=[]
    expensive = []
    if operand2 == "none":
      for prod in range(0,len(json_data)):
        val2 = json_data['similar_products'].values[prod]
        if(type(val2)==type(2.4)):
          continue
        v_dump = json.dumps(val2)
        v_json_obj = json.loads(v_dump)
        a1 = v_json_obj.get('meta')
        b1= a1.get("total_results")
        if(b1==0):
          continue
        v_price_dict = json_data['price'].values[prod]
        dump_v = json.dumps(v_price_dict)
        price_json_obj_v = json.loads(dump_v)
        get_basket_price = price_json_obj_v.get('basket_price')
        NAP_basket_price = get_basket_price.get('value')
        a = v_json_obj.get('website_results')
        b= a.get("5d0cc7b68a66a100014acdb0")
        c = b.get("knn_items")
        if(len(c)==1):
          d= c[0]["_source"]
          e = d.get("price")
          f = e.get("basket_price")
          g = f.get("value")
          if(NAP_basket_price>g):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        b= a.get("5da94e940ffeca000172b12a")
        c = b.get("knn_items")
        if(len(c)==1):
          d= c[0]["_source"]
          e = d.get("price")
          f = e.get("basket_price")
          g = f.get("value")
          if(NAP_basket_price>g):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        b= a.get("5da94ef80ffeca000172b12c")
        c = b.get("knn_items")
        if(len(c)==1):
          d= c[0]["_source"]
          e = d.get("price")
          f = e.get("basket_price")
          g = f.get("value")
          if(NAP_basket_price>g):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        b= a.get("5da94f270ffeca000172b12e")
        c = b.get("knn_items")
        if(len(c)==1):
          d= c[0]["_source"]
          e = d.get("price")
          f = e.get("basket_price")
          g = f.get("value")
          if(NAP_basket_price>g):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
        b= a.get("5da94f4e6d97010001f81d72")
        c = b.get("knn_items")
        if(len(c)==1):
          d= c[0]["_source"]
          e = d.get("price")
          f = e.get("basket_price")
          g = f.get("value")
          if(NAP_basket_price>g):
            p_id = json_data['_id'].values[prod]
            expensive.append(p_id.get("$oid"))
            continue
    else:
        spec_data = json_data[json_data.Brand_name==operand2]
        for prod in range(0,len(spec_data)):
            val2 = spec_data['similar_products'].values[prod]
            if(type(val2)==type(2.4)):
              continue
            v_dump = json.dumps(val2)
            v_json_obj = json.loads(v_dump)
            a1 = v_json_obj.get('meta')
            b1= a1.get("total_results")
            if(b1==0):
              continue
            v_price_dict = spec_data['price'].values[prod]
            dump_v = json.dumps(v_price_dict)
            price_json_obj_v = json.loads(dump_v)
            get_basket_price = price_json_obj_v.get('basket_price')
            NAP_basket_price = get_basket_price.get('value')
            a = v_json_obj.get('website_results')
            b= a.get("5d0cc7b68a66a100014acdb0")
            c = b.get("knn_items")
            if(len(c)==1):
              d= c[0]["_source"]
              e = d.get("price")
              f = e.get("basket_price")
              g = f.get("value")
              if(NAP_basket_price>g):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            b= a.get("5da94e940ffeca000172b12a")
            c = b.get("knn_items")
            if(len(c)==1):
              d= c[0]["_source"]
              e = d.get("price")
              f = e.get("basket_price")
              g = f.get("value")
              if(NAP_basket_price>g):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            b= a.get("5da94ef80ffeca000172b12c")
            c = b.get("knn_items")
            if(len(c)==1):
              d= c[0]["_source"]
              e = d.get("price")
              f = e.get("basket_price")
              g = f.get("value")
              if(NAP_basket_price>g):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            b= a.get("5da94f270ffeca000172b12e")
            c = b.get("knn_items")
            if(len(c)==1):
              d= c[0]["_source"]
              e = d.get("price")
              f = e.get("basket_price")
              g = f.get("value")
              if(NAP_basket_price>g):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue
            b= a.get("5da94f4e6d97010001f81d72")
            c = b.get("knn_items")
            if(len(c)==1):
              d= c[0]["_source"]
              e = d.get("price")
              f = e.get("basket_price")
              g = f.get("value")
              if(NAP_basket_price>g):
                p_id = spec_data['_id'].values[prod]
                expensive.append(p_id.get("$oid"))
                continue 
    return expensive

def higher_price_list(json_data):
  l1_=[]
  higher = []
  for prod in range(0,len(json_data)):
    val2 = json_data['similar_products'].values[prod]
    if(type(val2)==type(2.4)):
      continue
    v_dump = json.dumps(val2)
    v_json_obj = json.loads(v_dump)
    a1 = v_json_obj.get('meta')
    b1= a1.get("total_results")
    if(b1==0):
      continue
    v_price_dict = json_data['price'].values[prod]
    dump_v = json.dumps(v_price_dict)
    price_json_obj_v = json.loads(dump_v)
    get_nap_offer_price = price_json_obj_v.get('basket_price')
    NAP_offer_price = get_nap_offer_price.get('value')
    a = v_json_obj.get('website_results')
    b= a.get("5d0cc7b68a66a100014acdb0")
    c = b.get("knn_items")
    if(len(c)==1):
      d= c[0]["_source"]
      e = d.get("price")
      f = e.get("basket_price")
      g = f.get("value")
      h = g/10
      if(NAP_offer_price>g+h):
        p_id = json_data['_id'].values[prod]
        higher.append(p_id.get("$oid"))
  return higher

@app.route('/', methods=['POST'])

def Process_inp():
    #request = { "query_type": "discounted_products_list", "filters": [{ "operand1": "discount", "operator": "==", "operand2": 69.93 }] }
    data = request.get_json(force=True)
    request_data = json.dumps(data)
    request_data_obj = json.loads(request_data)
    query = request_data_obj.get('query_type')
    if query == "discounted_products_list":
        filters = request_data_obj.get('filters')
        op1=filters[0]['operand1']
        op=filters[0]['operator']
        op2=filters[0]['operand2']
        l_=nap_product_discount(Products_data,query,op,op1,op2)
        output = {'discounted_products_list': l_}
        return jsonify(output)
    if query == "discounted_products_count|avg_discount":
        filters = request_data_obj.get('filters')
        op1=filters[0]['operand1']
        op=filters[0]['operator']
        op2=filters[0]['operand2']
        output=Brand_avg_discount(Products_data,query,op,op1,op2)
        return jsonify(output)
    if query == "expensive_list":
        filters = request_data_obj.get('filters')
        if (filters):
            op1=filters[0]['operand1']
            op=filters[0]['operator']
            op2=filters[0]['operand2']
            exp_lst = ex_list(Products_data,op2)
            output = {'expensive_list': exp_lst}
            return jsonify(output)
        else:
            exp_lst = ex_list(Products_data)
            output = {'expensive_list': exp_lst}
            return jsonify(output)
    if query == "competition_discount_diff_list":
        hig_lst = higher_price_list(Products_data)
        output = {'competition_discount_diff_list': hig_lst}
        return jsonify(output)
        
prepare_dataset('netaporter_gb_similar.json')

if __name__ == '__main__':
    app.run(port = 5000, debug=True)




