#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    @Time      : 2022/3/18 17:01
    @Author    : fab
    @File      : modinParallel.py
    @Email     : xxxx@etransfar.com
"""


def extract(cnd_json):
    while 'true' in cnd_json or 'false' in cnd_json:
        cnd_json = cnd_json.replace('true', 'True').replace('false', 'False')

    cnd_json1 = eval(cnd_json)
    startCity, endProvince, endCity, endRegion, endLoc = "Miss", "Miss", "Miss", "Miss", "Miss"
    feeType, carrier, carrier_code = "Miss", "Miss", "Miss"
    for d in cnd_json1:
        for key, val in d.items():
            if "tln_platform_order.start_address" == val:
                startCity = d["value"]
            if "tln_platform_order.end_address" == val:
                # print( d["value"])
                splits = d["value"].split(",")
                endProvince = splits[0]
                endCity = splits[1]
                endRegion = splits[-1]
                endLoc = endRegion
                if "市辖区" in endRegion:
                    endRegion = endCity
            if "tln_platform_order.carrier" == val:
                carrier = d["value"]
            if "tln_platform_order.carrier_code" == val:
                carrier_code = d["value"]
            if "common_order.type" == val:
                feeType = d["value"]
    return [startCity, endProvince, endCity, endRegion,
            endLoc, feeType, carrier, carrier_code]


if __name__ == "__main__":
    import time

    # import modin.pandas as pd
    # import swifter
    # swifter.register_modin()
    #
    # start_time = time.time()
    # fee_rule_detail_condition = pd.read_csv(
    #     "C:/Users/admin/Desktop/jupyter/huoyunwang/dataset/fee_rule_detail_condition.csv")
    # fee_rule_detail_condition = fee_rule_detail_condition[fee_rule_detail_condition["party_id"] == 9527]
    # lineFee = fee_rule_detail_condition["condition_json"].swifter.apply(extract)
    # columns = [
    #     "from_city",
    #     "end_province",
    #     "end_city",
    #     "end_region",
    #     "end_loc",
    #     "fee_type",
    #     "carrier",
    #     "carrier_code"]
    # lineFee = pd.DataFrame(list(lineFee), columns=columns)
    # print(f"total spend time is {time.time() - start_time} second.")  # 32.45
    # print(lineFee.head(2))

    import pandas as pd
    start_time = time.time()
    fee_rule_detail_condition = pd.read_csv(
        "C:/Users/admin/Desktop/jupyter/huoyunwang/dataset/fee_rule_detail_condition.csv")
    fee_rule_detail_condition = fee_rule_detail_condition[fee_rule_detail_condition["party_id"] == 9527]
    lineFee = fee_rule_detail_condition["condition_json"].apply(extract)
    columns = [
        "from_city",
        "end_province",
        "end_city",
        "end_region",
        "end_loc",
        "fee_type",
        "carrier",
        "carrier_code"]
    lineFee = pd.DataFrame(list(lineFee), columns=columns)
    print(f"total spend time is {time.time() - start_time} second.")    # 82.6
    print(lineFee.head(2))
