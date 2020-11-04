# 네이버 검색 API예제는 블로그를 비롯 전문자료까지 호출방법이 동일하므로 blog검색만 대표로 예제를 올렸습니다.
# 네이버 검색 Open API 예제 - 블로그 검색
import os
import sys
import urllib.request
import json
import csv
import pandas as pd
from pandas import DataFrame
import time
import winsound
import re

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
baseurl = os.path.dirname(os.path.abspath(__file__))
from com_dayoung_api.utils.file_helper import FileReader

'''
10/23 rev1.0
5가지 추가 필요
1. 중복 데이터의 제거 기능
    - 제목, 년도, 감독 의 값이 동일하면 df에서 제거 (csv는 유지? )
null 값의 대체 입력 기능
    - ui 단에서 사진이 짤려서 나와서 사진 크기에 영향 줌
2. 정리하고 주석 달기 
    - 파일 변경 같은 부분(주석 달린 부분) filehelper로 옮기기
3. merge_df 단에서 네이버 영화 df를 읽어오지 말고 csv로 읽어오기?
    - 항상 api를 검색하면 시간이 오래걸림 csv를 만들고 df로 읽어오게 변환
4. 검색 정확도 높이기
    - 특수 문자, 한자 등 제거 하여 검색 정확도 높이기?
    - 검색 안되는 값이 주로 옜날 영화이니 year컬럼 기준으로 index삭제 하기 (ex 1980 이전은 삭제)
5. 검색 delay 주기
    - 검색시에 데이터가 너무 많고, 집에 인터넷이 느린지 에러가 발생
    - c 언어 처럼 delay를 줄 수 있는지 찾아 보기
6. bit pj api로 옮기기 (파일명 search ??)
    - bit pj로 옮기고 실행 마무리 하기 (최종 단계) / git api 아이디 비번 삭제!
'''

'''
10/27 rev1.1
추가 내용
검색 delay 주기
    - delay 0.1초 추가 완료(네이버에서 초당 10개로 제한함)
검색 정확도 높이기
    - kmdb 년도와 네이버 년도가 1년 정도 차이 나는 데이터가 있어서, 년도 검색 범위를 +-1로 줌
검색 case 정리
    - 중복되거나 필요 없던 검색 case 삭제
코드 정리 및 주석 정리
'''

'''
11/1 rev2.0
추가 내용
1. bit pj api로 이동 (기존은 git에 올림)
2. 검색 진행 네이버 api 문제로 (한번에 많이 돌리면 응답을 안 한다.) 5000번씩 끊어서 데이터 저장 완료
3. merge_df 단에서 csv로 읽어와서 merge하게 변환 (hook의 step 주석 참조)

추가 필요(시간 날 때..)
- 수동으로 끊어서 저장 하는 것 자동으로 되게 변경
- naver csv merge 부분도 하드코딩 한 것 변경
'''

class NaverMovie:
    def __init__(self):
        self.reader = FileReader() 
        self.client_id = ""                              # 네이버 api id (git 삭제!!!)
        self.client_secret = ""                                # 네이버 api secret (git 삭제!!!)
        self.none_image = '../images/none_image.jpg'    # 검색 결과에서 image가 없을 경우 사용할 image

    def hook(self):
        print('*'*10, 'START', '*'*10)
        
        kmdb_df = self.read_kmdb_csv()                                     # step1 : kmdb_csv 읽어와서 df으로 전환 (return : df)
        # kmdb_movie_title_list = self.get_title_list(kmdb_df)             # step2 : 검색을 위한 제목 list 추출 (return : list)
        # kmdb_movie_year_list = self.get_year_list(kmdb_df)               # step3 : 검색을 위한 년도 list 추출 (return : list)

        # print('*'*30)

        # movie_dict = self.search_naver_movie(kmdb_movie_title_list,\
        #                                         kmdb_movie_year_list)    # step4 : 네이버 영화 api 검색 진행 (save : csv)
        # self.naver_dict_to_csv(movie_dict)                               # step5 : 네이버 영화 검색 dict df 전환 및 csv 저장 (save : csv)
        # self.naver_csv_merge()                                           # step5-1 : 네이버 api 문제로 5000개씩 끊어서 검색 한것 merge ( save : csv)

        #################################### step 5 완료 후 step6,7,8 주석 풀고 step 2,3,4,5는 막고 진행(네이버 api 할당량 문제) ####################################

        naver_movie_df = self.read_naver_movie_csv()                       # step6 : 저장한 네이버 영화 csv 읽어오기 ( return : df)

        merge_df = self.merge_csv_to_df(kmdb_df, naver_movie_df)           # step7 : ui json 생성을 위한 kmdb_csv(df)와 네이버 영화 검색(df) merge ( save : csv, return : df)
        self.df_to_ui_json(merge_df)                                       # step8 : merge_df의 column 삭제 및 정렬 후 json파일 저장 ( save : json)     
        print('*'*10, 'E N D', '*'*10)

    def read_kmdb_csv(self):
        reader = self.reader
        reader.context = os.path.join(baseurl, 'data')
        reader.fname = 'kmdb_csv.csv'
        reader.new_file()
        kmdb_df = reader.csv_to_dframe_euc_kr()
        return kmdb_df

    def read_naver_movie_csv(self):
        reader = self.reader
        reader.context = os.path.join(baseurl, 'saved_data')
        reader.fname = 'naver_movie_search_merge.csv'
        reader.new_file()
        naver_movie_df = reader.csv_to_dframe_utf_8()
        return naver_movie_df

    def get_title_list(self, data):
        kmdb_movie_title_list = []
        for i in data['title']:
            kmdb_movie_title_list.append(i)

        return kmdb_movie_title_list

    def get_year_list(self, data):
        kmdb_movie_year_list = []
        for i in data['year']:
            kmdb_movie_year_list.append(i)

        return kmdb_movie_year_list

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # 영 화 검 색 # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

    def search_naver_movie(self, kmdb_movie_title_list, kmdb_movie_year_list):
        titlelist = []        # kmdb의 영화 이름이 저장될 list
        yearlist = []         # kmdb의 영화 년도가 저장될 list
        real_dict = {}        # 네이버영화의 json값을 저장할 dict
        movie_index = 0       # kmdb csv의 인덱스
        ui_json_id = 5481200  # ui_json의 인덱스 
        print('검색 시작!!!!!!!!!!!!!!!!!')
        
        for kmdb_year in kmdb_movie_year_list:
            yearlist.append(kmdb_year)     
        
        # for-1 start -> kmdb title로 naver title 검색
        for kmdb_title in kmdb_movie_title_list:
            titlelist.append(kmdb_title)

            time.sleep(0.1) # 검색 delay / 네이버에서 1초당 10건으로 제한
            print('*'*30)
            
            
            search_year_list = [yearlist[movie_index] - 1,
                                yearlist[movie_index],
                                yearlist[movie_index] + 1]  # kmdb와 +-로 1년 차이가 나는 값을 위해 조정

            print(f'영화 인덱스 : {movie_index}번')
            print(f'검색 제  목 : {kmdb_title}')                                        # 검색 영화 제목
            print(f'검색 연  도 : {search_year_list[0]} ~ {search_year_list[2]}')       # 검색 영화 연도

            encText = urllib.parse.quote(kmdb_title)
            display = "&display=100"
            yearfrom = (f"&yearfrom={search_year_list[0]}")
            yearto = (f"&yearto={search_year_list[2]}")
            url = "https://openapi.naver.com/v1/search/movie.json?query=" \
                                                                            + encText \
                                                                            + display \
                                                                            + yearfrom \
                                                                            + yearto
            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id",self.client_id)
            request.add_header("X-Naver-Client-Secret",self.client_secret)
            response = urllib.request.urlopen(request)
            rescode = response.getcode()
            
            # if-1 start -> 맞게 응답 할 경우 json 데이터 반환
            if(rescode==200):
                response_body = response.read().decode('utf-8')
                jsondata = json.loads(response_body)
                items = jsondata['items']   # 검색된 json 데이터
                temp_dict = {}              # 각 영화를 담을 임시 dict (to update -> real_dict)

                # if-2 start -> kmdb의 영화제목이 네이버 영화에 없는 경우(case0) / else 있는 경우
                if items == []:
                    real_data = {'title':'empty_value',
                                'link':'empty_value',
                                'image':self.none_image,
                                'subtitle':'empty_value',
                                'pubDate':'0000',
                                'director':'empty_value',
                                'actor':'empty_value',
                                'userRating':'empty_value',
                                'id':ui_json_id}                                    
                    temp_dict[movie_index] = real_data
                    real_dict.update(temp_dict)


                else:       # kmdb의 영화제목이 네이버 영화에 있는 경우
                    i = 0   # 네이버에 검색된 영화 갯수
                    
                    # for-2 start -> 검색된 영화들 case별 비교 하여 dict에 네이버 영화 정보 update
                    for i in range(len(items)):
                        titles = items[i]['title']
                        kmdb_titles_drop1 = titlelist[movie_index].replace(' ', '')         # kmdb 공백 제거
                        naver_titles_drop1 = titles.replace('<b>','').replace('</b>', '')   # 네이버 <b>, </b> 제거
                        naver_titles_drop2 = naver_titles_drop1.replace(' ', '')            # 네이버 공백 제거
                        real_data = []  # naver 영화 json의 value값 저장
                        '''
                        case0: kmdb의 영화 제목이 네이버에 없는 경우
                        case1: 제목 일  치 (네이버 공백 포함) / image는 없는 경우 none_image 대체
                        case2: 제목 일  치 (네이버 공백 제거) / image는 없는 경우 none_image 대체
                        case3: 제목 일  치 (네이버, kmdb 둘 다 공백 제거) / image는 없는 경우 none_image 대체
                        case4: 제목 불일치
                        '''

                        # if-3 start -> 각 case 위 주석 참조
                        if naver_titles_drop1 == titlelist[movie_index]:             
                            real_data = items[i]                                     
                            if items[i]['image'] == '':
                                real_data['image'] = self.none_image                 # none_image.jpg로 표시
                            userrating_naver = float(real_data['userRating'])/2      # 별점 10점 만점 -> 5점 만점
                            userrating_naver = '%0.1f' % float(userrating_naver)
                            real_data['userRating'] = userrating_naver              
                            real_data['id'] = ui_json_id                             # ui의 image card 인덱스 값
                            temp_dict[movie_index] = real_data                       # {kmdb의 title값 : naver영화 json value}
                            real_dict.update(temp_dict)
                            print('case1')
                        elif naver_titles_drop2 == titlelist[movie_index]:
                            real_data = items[i]                                  
                            if items[i]['image'] == '':
                                real_data['image'] = self.none_image               
                            userrating_naver = float(real_data['userRating'])/2    
                            userrating_naver = '%0.1f' % float(userrating_naver)
                            real_data['userRating'] = userrating_naver              
                            real_data['id'] = ui_json_id                         
                            temp_dict[movie_index] = real_data                   
                            real_dict.update(temp_dict)
                            print('case2')                            
                        elif naver_titles_drop2 == kmdb_titles_drop1:
                            real_data = items[i]                                     
                            if items[i]['image'] == '':
                                real_data['image'] = self.none_image                 
                            userrating_naver = float(real_data['userRating'])/2      
                            userrating_naver = '%0.1f' % float(userrating_naver)
                            real_data['userRating'] = userrating_naver              
                            real_data['id'] = ui_json_id                             
                            temp_dict[movie_index] = real_data                      
                            real_dict.update(temp_dict)
                            print('case3')   
                        else:
                            real_data = {'title':'empty_value',
                                        'link':'empty_value',
                                        'image':self.none_image,
                                        'subtitle':'empty_value',
                                        'pubDate':'0000',
                                        'director':'empty_value',
                                        'actor':'empty_value',
                                        'userRating':'empty_value',
                                        'id':ui_json_id}                                    
                            temp_dict[movie_index] = real_data
                            real_dict.update(temp_dict)
                            print('case4') 
                        # if-3 end
                    i += 1
                    # for-2 end
                ui_json_id += 100
                # if-3 end
            else:
                print("Error Code:" + rescode)
            # if-1 end
            movie_index += 1
            if len(real_dict) > 5000:   # 네이버 api 불안정으로 5000번씩 끊어서 저장
                break
        # for-1 end
        winsound.Beep(700, 5000)        # 검색 완료 되면 beep음 발생
        print('*'*30)
        print('검색 끝!!!!!!!!!!!!!!!!!!')
        return real_dict
        
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # 영 화 검 색 # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

    # 검색 결과 csv으로 저장
    def naver_dict_to_csv(self, movie_dict):
        print('*****to dataframe 진행*****')
        reader = self.reader
        reader.context = os.path.join(baseurl, 'saved_data')
        reader.fname = 'naver_movie_search11.csv'
        reader.new_file()

        myframe = reader.dict_to_dframe(movie_dict)
        # title, link, image, subtitle, pubDate, director, actor, userRating
        navermovie_index = \
            {'title':'title_naver',
            'link':'link_naver',
            'image':'image_naver',
            'subtitle':'subtitle_naver',
            'pubDate':'pubdate_naver',
            'director':'director_naver',
            'actor':'actor_naver',
            'userRating':'userrating_naver'}               
        myframe = myframe.rename(index=navermovie_index)
        myframe = myframe.T
        # print(myframe)
        reader.dframe_to_csv(myframe)
        print('*****to dataframe 완료*****')        

    # 저장된 csv merge
    def naver_csv_merge(self):
        reader = self.reader
        reader.context = os.path.join(baseurl, 'saved_data')
        
        reader.fname = 'naver_movie_search1.csv'
        reader.new_file()
        df1 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search2.csv'
        reader.new_file()
        df2 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search3.csv'
        reader.new_file()
        df3 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search4.csv'
        reader.new_file()
        df4 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search5.csv'
        reader.new_file()
        df5 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search6.csv'
        reader.new_file()
        df6 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search7.csv'
        reader.new_file()
        df7 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search8.csv'
        reader.new_file()
        df8 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search9.csv'
        reader.new_file()
        df9 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search10.csv'
        reader.new_file()
        df10 = reader.csv_to_dframe_utf_8()

        reader.fname = 'naver_movie_search11.csv'
        reader.new_file()
        df11 = reader.csv_to_dframe_utf_8()
     
        naver_df_merge = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11], axis=0, ignore_index=True)
        naver_df_merge.to_csv(baseurl + '/saved_data/naver_movie_search_merge.csv', encoding='utf-8')

    # kmdb csv와 naver csv merge 및 df 변환 ( return : df )
    def merge_csv_to_df(self, kmdb_df, naver_movie_df):
        print('*****df merge 진행*****')
        merge_df = pd.concat([kmdb_df, naver_movie_df], axis=1)
        merge_df.to_csv(baseurl + '/saved_data/kmdb_naver_merge.csv', encoding='utf-8')
        print('*****df merge 완료*****')        
        return merge_df

    # df를 ui의 movie.json에 맞게 df 가공 및 .json 파일 저장
    def df_to_ui_json(self, merge_df):
        print('*****to json 진행*****')

        '''
        ***** 현재 columns *****
        title,
        genre,
        country,
        year,
        company,
        director,
        actor,
        date,
        running_time,
        keyword,
        plot,
        title_naver,
        link_naver,
        image_naver,
        subtitle_naver,
        pubdate_naver,
        director_naver,
        actor_naver,
        userrating_naver
        '''
        '''
        ***** 만들 json sample *****
        {
        "id": 900,
        "title": "Resident Evil",
        "subtitle": "Vendetta",
        "description": "Chris Redfield enlists the help of Leon S. Kennedy and Rebecca Chambers to stop a death merchant, with a vengeance, from spreading a deadly virus in New York.",
        "year": 2014,
        "imageUrl": "../images/담보.jpg",
        "rating": 4.2
        }
        '''

        # 필요 없는 column drop
        drop_df = merge_df.drop \
                    (['country', 
                    'company', 
                    'actor', 
                    'date', 
                    'running_time',
                    'plot',  
                    'link_naver',  
                    'subtitle_naver',
                    'actor_naver'], axis=1)
        
        # ui json 형식에 맞게 sorting
        sort_df = drop_df[['id', 'title', 'title_naver', 'genre', 'keyword', 'image_naver', 'year', 'pubdate_naver', 'userrating_naver', 'director', 'director_naver']]
        
        ui_json_columns = {
            'genre':'subtitle',
            'keyword':'description',
            'image_naver':'imageUrl',
            'userrating_naver':'rating'
        }
        # ui json과 동일하게 colunm 이름 변경
        rename_df = sort_df.rename(columns=ui_json_columns)
        
        ui_json = DataFrame.to_json(rename_df, orient = 'records', force_ascii=False, indent=8)
        # print(ui_json)
        save_json = json.loads(ui_json)         # --> filehelper로 옮기기
        with open(baseurl + '/saved_data/movies.json', 'w', encoding='utf-8') as make_json:       
            json.dump(save_json, make_json, ensure_ascii=False, indent="\t")
        print('*****to json 완료*****')

if __name__ == "__main__":
    search_naver = NaverMovie()
    search_naver.hook()
