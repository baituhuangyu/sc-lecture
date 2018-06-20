# encoding: utf-8

import aiohttp
import asyncio


# i = 0

async def fetch(session, url):
    async with session.get(url) as response:
        # global i
        # i += 1
        # print(i)
        return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, 'http://localhost:8080/')
        print(html)

        return html

if __name__ == '__main__':
    import datetime
    t1 = datetime.datetime.now()
    loop = asyncio.get_event_loop()
    tks = [main() for _ in range(10)]

    rst = loop.run_until_complete(asyncio.gather(*tks))
    ff = datetime.datetime.now() - t1
    print(rst)
    print('\n')
    print(ff)

