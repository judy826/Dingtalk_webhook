from flask import Flask, Response
from flask_cors import CORS
from modules.excel_snap import excel_snap_def
from modules.insert_value import insert_value_def
from modules.read_value import get_data_writeoff
from modules.save_data import save_data, select_data

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.debug = True
app.config['JSON_AS_ASCII'] = False  # 解决flask接口中文数据编码问题


@app.route('/picture/<search_date>.png')
def get_picture(search_date):
    path = '销售日报-模板.xlsx'
    sheet_list = ['金融']
    snap_range = 'A1:F30'
    df_select = select_data(search_date)
    if len(df_select) == 1:
        image = df_select['photo'].iloc[0]
    else:
        # search_date = '2022-05-28'
        print('首次使用该日期，请耐心等待！')
        search_date_mon = search_date.split('-')[0] + '-' + search_date.split('-')[1]
        df = get_data_writeoff(search_date)
        df_target = get_data_writeoff(search_date_mon)
        new_path = insert_value_def(df, df_target, path, search_date)
        img_path = excel_snap_def(new_path, search_date, sheet_list, snap_range)
        img_path = img_path + '{}.png'.format(search_date)
        with open(img_path, 'rb') as f:
            image = f.read()
        save_data(search_date, image)
    resp = Response(image, content_type="image/png")
    return resp


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8887, debug=True)
