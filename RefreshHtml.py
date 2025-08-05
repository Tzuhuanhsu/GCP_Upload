import configparser
import sys
import pandas
import GCPTool

# Html 廳館列表 內容
html_content = ''' const gameList = [{}];'''
# 對應環境的 html 檔名
html_fileName = {"dev":"index-dev.html","dev-fish-test":"dev-fish-test.html","new-dev":"index-dev-new.html"}
# 工具:用來更刷新Dev and PreRelease的html
# 直接調整Excel的欄位就可以
class RefreshHtml:
    def __init__(self, param) -> None:
        # 檢查參數
        if param[0] not in["dev","dev-fish-test", "new-dev"]:
            print(f"參數錯誤:{param[0]}")
            sys.exit()
        # 抓取 ini setting
        config = configparser.ConfigParser()
        config.read("Config.ini")
        self.Config = config["Setting"]

        # 抓取Excel的設定
        excel_data = pandas.read_excel(self.Config["ReadExcel"], sheet_name=param[0])
        elements = ""
        for index in excel_data["game_code"].to_dict():
            element = "{"+'gameId:{},\ngame_code:"{}",\ntitle:"{}",\nimg:"{}",'.format(index, excel_data.loc[index]["game_code"], excel_data.loc[index]["title"], excel_data.loc[index]["img"])
            if "url" in excel_data.columns and excel_data.loc[index]["url"]:
                element +="url:\"{}\"".format(excel_data.loc[index]["url"])
            element = element + "}"
            if index !=  len(excel_data["game_code"].to_dict())-1:
                element = element+"," 
            elements = elements + element

        # 建立新的html
        template = "Template.html"
        if param[0] == "new-dev" or param[0] == "dev-fish-test":
          template = "Template-new.html"
        with open(template, 'r', encoding='utf-8') as file:
            content = file.read()
            new_content =  content.replace("{ ReplaceNewUrl }",html_content.format(elements))  
            with open(f'{html_fileName[param[0]]}', 'w', encoding='utf-8') as file:
                file.write(new_content)

        # 上傳到GCP
        gcp_tool = GCPTool.GCPTool(None)
        gcp_tool.readConfig("Test")
        gcp_tool.bucketName = self.Config["TargetBucket"]
        gcp_tool.upload_file( gcp_tool.getBucket(), f"{html_fileName[param[0]]}",
                                      f".\\{html_fileName[param[0]]}")
        print(f'Refresh Html {html_fileName[param[0]]} Done')

if __name__ == "__main__":
    RefreshHtml(sys.argv[1:])