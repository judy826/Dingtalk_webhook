import excel2img
import os


def excel_snap_def(excel_file_save, date_value, sheet_list, excel_range):
    if os.path.dirname(excel_file_save) == "":
        excel_file_save = os.getcwd() + "//" + excel_file_save
    img_save = os.getcwd() + "//截图//"
    if not os.path.exists(img_save):
        os.makedirs(img_save)
    # 保存为图片。
    try:
        print("开始截图，请耐心等待。。。")
        for i in range(len(sheet_list)):
            excel2img.export_img(excel_file_save, img_save + '//' + date_value + ".png", sheet_list[i], excel_range)
    except:
        print("没有截图成功！！！")
    else:
        print("截图成功！【保存在" + img_save + "】")
    return img_save
