import openpyxl
import os


def insert_value_def(df, df_target, path, search_date):
    myexcel = openpyxl.load_workbook(path)
    # sheet_list = myexcel.sheetnames
    # wb = myexcel.active
    # worksheet= workbook.get_sheet_by_name('总览')
    ws_sheet = myexcel['金融']
    ws_sheet['C4'] = df['day_total_volume'].iloc[0]
    # 本品牌整车销量
    ws_sheet['C16'] = df['day_total_volume'].iloc[1]
    ws_sheet['C17'] = df['month_total_volume'].iloc[1]
    ws_sheet['C18'] = df['year_total_volume'].iloc[1]
    # 本品牌合同量
    ws_sheet['C11'] = df['day_total_volume'].iloc[6]
    ws_sheet['C12'] = df['month_total_volume'].iloc[6]
    ws_sheet['C14'] = df['year_total_volume'].iloc[6]
    # 全品新车合同量
    ws_sheet['D11'] = df['day_total_volume'].iloc[7]
    ws_sheet['D12'] = df['month_total_volume'].iloc[7]
    ws_sheet['D14'] = df['year_total_volume'].iloc[7]
    # 全品二手车合同量
    ws_sheet['E11'] = df['day_total_volume'].iloc[5]
    ws_sheet['E12'] = df['month_total_volume'].iloc[5]
    ws_sheet['E14'] = df['year_total_volume'].iloc[5]
    # 本品牌申请量
    ws_sheet['C4'] = df['day_total_volume'].iloc[9]
    ws_sheet['C5'] = df['month_total_volume'].iloc[9]
    ws_sheet['C6'] = df['year_total_volume'].iloc[9]
    # 全品新车申请量
    ws_sheet['D4'] = df['day_total_volume'].iloc[8]
    ws_sheet['D5'] = df['month_total_volume'].iloc[8]
    ws_sheet['D6'] = df['year_total_volume'].iloc[8]
    # 全品二手车申请量
    ws_sheet['E4'] = df['day_total_volume'].iloc[10]
    ws_sheet['E5'] = df['month_total_volume'].iloc[10]
    ws_sheet['E6'] = df['year_total_volume'].iloc[10]
    # 本品牌核准量
    ws_sheet['C7'] = df['day_total_volume'].iloc[3]
    ws_sheet['C8'] = df['month_total_volume'].iloc[3]

    # 全品新车核准量
    ws_sheet['D7'] = df['day_total_volume'].iloc[2]
    ws_sheet['D8'] = df['month_total_volume'].iloc[2]

    # 全品二手车核准量
    ws_sheet['E7'] = df['day_total_volume'].iloc[4]
    ws_sheet['E8'] = df['month_total_volume'].iloc[4]

    # 目标量
    ws_sheet['I2'] = df_target['month_total_volume'].iloc[0]
    ws_sheet['J2'] = df_target['month_total_volume'].iloc[1]
    ws_sheet['K2'] = df_target['month_total_volume'].iloc[2]
    ws_sheet['L2'] = df_target['month_total_volume'].iloc[3]
    ws_sheet['I3'] = df_target['year_total_volume'].iloc[0]
    ws_sheet['J3'] = df_target['year_total_volume'].iloc[1]
    ws_sheet['K3'] = df_target['year_total_volume'].iloc[2]
    ws_sheet['L3'] = df_target['year_total_volume'].iloc[3]

    # 计算公式
    ws_sheet['F4'] = '=C4+D4+E4'
    ws_sheet['F5'] = '=C5+D5+E5'
    ws_sheet['F6'] = '=C6+D6+E6'
    ws_sheet['F7'] = '=C7+D7+E7'
    ws_sheet['F8'] = '=C8+D8+E8'
    ws_sheet['F9'] = '=F8/F5'
    ws_sheet['F10'] = '=F9'
    ws_sheet['F11'] = '=C11+D11+E11'
    ws_sheet['F12'] = '=C12+D12+E12'
    ws_sheet['F13'] = '=F12/M2'
    ws_sheet['F14'] = '=C14+D14+E14'
    ws_sheet['F15'] = '=F14/M3'

    ws_sheet['C19'] = '=C4/C16'
    ws_sheet['C20'] = '=C11/C16'
    ws_sheet['C21'] = '=C5/C17'
    ws_sheet['C22'] = '=C12/C17'
    ws_sheet['C23'] = '=C12/C17/L2/K2'
    ws_sheet['C24'] = '=C14/C18'
    ws_sheet['C25'] = '=C14/C18/L3/K3'

    ws_sheet['C9'] = '=C8/C5'
    ws_sheet['C10'] = '=C9'
    ws_sheet['D9'] = '=D8/D5'
    ws_sheet['D10'] = '=D9'
    ws_sheet['E9'] = '=E8/E5'
    ws_sheet['E10'] = '=E9'
    ws_sheet['F9'] = '=F8/F5'
    ws_sheet['F10'] = '=F9'

    ws_sheet['C13'] = '=C12/L2'
    ws_sheet['D13'] = '=D12/J2'
    ws_sheet['E13'] = '=E12/I2'
    ws_sheet['F13'] = '=F12/M2'

    ws_sheet['C15'] = '=C14/L3'
    ws_sheet['D15'] = '=D14/J3'
    ws_sheet['E15'] = '=E14/I3'
    ws_sheet['F15'] = '=F14/M3'

    ws_sheet['M2'] = '=I2+J2+L2'
    ws_sheet['M3'] = '=I3+J3+L3'

    # 插入表头
    search_date_split = search_date.split('-')
    date_value = search_date_split[0] + '年' + search_date_split[1] + '月' + search_date_split[2] + '日'
    ws_sheet['A1'] = date_value

    # 保存数据
    data_save = os.getcwd() + "//数据填充文件//"
    if not os.path.exists(data_save):
        os.makedirs(data_save)
    path = data_save + str(search_date) + '.xlsx'
    myexcel.save(path)

    return path
