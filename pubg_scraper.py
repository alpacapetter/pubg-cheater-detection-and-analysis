import requests 
from requests_html import HTMLSession
from bs4 import BeautifulSoup

user = "watermeloong"
url = "https://pubg.op.gg/user/" + user
result = requests.get(url)

# soup = BeautifulSoup(result.text, 'html.parser')
# recent_match_kdr = soup.select('div[class="recent-matches__stat-value recent-matches__stat-value--good"]')
# recent_match_kdr1 = soup.find_all('div')
# print(recent_match_kdr1)

s = HTMLSession()

def getdata(url):
    r = s.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup

def getMore(soup):
    for _ in range(5):
        more_button = soup.find('button', {'class': 'total-played-game__btn total-played-game__btn--more'})



#1. extract userId
#2. put userId in proper url
#3. continue to get next data page with the last "offset"