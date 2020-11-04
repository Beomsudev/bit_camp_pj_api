import os
import sys
import urllib.request


client_id = ""
client_secret = ""
encText = urllib.parse.quote("파파라치")
display = "&display=100"
yearfrom = "&yearfrom=1989"
yearto = "&yearto=1991"
url = "https://openapi.naver.com/v1/search/movie.json?query=" + encText + display #+ yearfrom + yearto   # json 결과

request = urllib.request.Request(url)
request.add_header("X-Naver-Client-Id",client_id)
request.add_header("X-Naver-Client-Secret",client_secret)
response = urllib.request.urlopen(request)
rescode = response.getcode()
print(response)
print(rescode)
if(rescode==200):
    response_body = response.read()
    print(response_body.decode('utf-8'))
else:
    print('2')
    print("Error Code:" + rescode)
    
