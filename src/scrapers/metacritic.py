import requests
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


def get_albums(year):
    url = f'https://www.metacritic.com/browse/albums/score/metascore/year/filtered?view=condensed&year_selected={year}&sort=desc'
    response = requests.get(url, headers=headers)
    html = BeautifulSoup(response.text, 'html.parser')

    albums = [item.find('a') for item in html.select('div.product_title')]

    for album in albums:
        link = album.get('href')
        title = album.text.strip().replace(',', ' ')
        artist, genre, rating = get_details(link)
        print(f'{artist},{title},{genre},{year},{rating}')


def get_data(html, details_type):
    spans = [d for d in html.select('span') if d.get('itemprop') and d.get('itemprop') == details_type]

    if len(spans) > 0:
        return spans[0].text.strip()
    else:
        return ""


def get_artist(html):
    spans = [d for d in html.select('span.band_name') if d.get('itemprop') and d.get('itemprop') == 'name']

    if len(spans) > 0:
        return spans[0].text.strip()
    else:
        return ""


def get_details(link):
    response = requests.get(f'https://www.metacritic.com{link}', headers=headers)
    html = BeautifulSoup(response.text, 'html.parser')
    artist = get_artist(html).replace(',', ' ')
    genre = get_data(html, 'genre')
    rating = get_data(html, 'ratingValue')

    return artist, genre, rating

print('Artist,Album,Genre,Year,Rating')

for year in range(2015, 2019):
    get_albums(year)


# print(html.prettify())



