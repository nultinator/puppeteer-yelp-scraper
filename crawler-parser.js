const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;

console.log("api key:", API_KEY);


async function scrapeSearchResults(browser, keyword, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const formattedKeyword = keyword.replace(" ", "+");
        const page = await browser.newPage();
        try {
            const url = `https://www.yelp.com/search?find_desc=${formattedKeyword}&find_loc=${location}`;
    
            await page.goto(url);
            console.log(`Successfully fetched: ${url}`);

            const divCards = await page.$$("div[data-testid='serp-ia-card']");

            for (const divCard of divCards) {

                const cardText = await page.evaluate(element => element.textContent, divCard);
                const img = await divCard.$("img");
                const name = await page.evaluate(element => element.getAttribute("alt"), img);                
                const nameRemoved = cardText.replace(name, "");

                let sponsored = isNaN(nameRemoved[0]);
                
                let rank = 0;
                if (!sponsored) {
                    rankString = nameRemoved.split(".");
                    rank = Number(rankString[0]);
                }

                let rating = 0.0;
                const hasRating = await divCard.$("div span[data-font-weight='semibold']");
                if (hasRating) {
                    const ratingText = await page.evaluate(element => element.textContent, hasRating);
                    if (ratingText.length > 0) {
                        rating = Number(ratingText);
                    }
                }

                let reviewCount = "0";
                if (cardText.includes("review")) {
                    reviewCount = cardText.split("(")[1].split(")")[0].split(" ")[0];
                }

                const aElement = await divCard.$("a");
                const link = await page.evaluate(element => element.getAttribute("href"), aElement);
                const yelpUrl = `https://www.yelp.com${link.replace("https://proxy.scrapeops.io", "")}`

                const searchData = {
                    name: name,
                    sponsored: sponsored,
                    stars: rating,
                    rank: rank,
                    review_count: reviewCount,
                    url: yelpUrl
                }

                console.log(searchData);
            }


            success = true;
        } catch (err) {
            console.log(`Error: ${err}, tries left ${retries - tries}`);
            tries++;
        } finally {
            await page.close();
        } 
    }
}

async function startScrape(keyword, location, retries) {
    const browser = await puppeteer.launch()

    await scrapeSearchResults(browser, keyword, location, retries);    

    await browser.close();
}


async function main() {
    const keywords = ["restaurants"];
    const concurrencyLimit = 4;
    const pages = 1;
    const location = "uk";
    const retries = 3;
    const aggregateFiles = [];

    for (const keyword of keywords) {
        console.log("Crawl starting");
        await startScrape(keyword, location, retries);
        console.log("Crawl complete");
        aggregateFiles.push(`${keyword.replace(" ", "-")}.csv`);
    }

}


main();