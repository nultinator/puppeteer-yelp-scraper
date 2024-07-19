const puppeteer = require("puppeteer");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csvParse = require("csv-parse");
const fs = require("fs");

const API_KEY = JSON.parse(fs.readFileSync("config.json")).api_key;

console.log("api key:", API_KEY);

async function writeToCsv(data, outputFile) {
    if (!data || data.length === 0) {
        throw new Error("No data to write!");
    }
    const fileExists = fs.existsSync(outputFile);

    const headers = Object.keys(data[0]).map(key => ({id: key, title: key}))

    const csvWriter = createCsvWriter({
        path: outputFile,
        header: headers,
        append: fileExists
    });
    try {
        await csvWriter.writeRecords(data);
    } catch (e) {
        throw new Error("Failed to write to csv");
    }
}

function range(start, end) {
    const array = [];
    for (let i=start; i<end; i++) {
        array.push(i);
    }
    return array;
}

function getScrapeOpsUrl(url, location="us") {
    const params = new URLSearchParams({
        api_key: API_KEY,
        url: url,
        country: location,
        residential: true,
    });
    return `https://proxy.scrapeops.io/v1/?${params.toString()}`;
}

async function scrapeSearchResults(browser, keyword, pageNumber, location="us", retries=3) {
    let tries = 0;
    let success = false;

    while (tries <= retries && !success) {
        
        const formattedKeyword = keyword.replace(" ", "+");
        const page = await browser.newPage();
        try {
            const url = `https://www.yelp.com/search?find_desc=${formattedKeyword}&find_loc=${location}&start=${pageNumber*10}`;
    
            const proxyUrl = getScrapeOpsUrl(url, location);

            await page.goto(proxyUrl);
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

                await writeToCsv([searchData], `${keyword.replace(" ", "-")}.csv`);
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

async function startScrape(keyword, pages, location, concurrencyLimit, retries) {
    const pageList = range(0, pages);

    const browser = await puppeteer.launch()

    while (pageList.length > 0) {
        const currentBatch = pageList.splice(0, concurrencyLimit);
        const tasks = currentBatch.map(page => scrapeSearchResults(browser, keyword, page, location, retries));

        try {
            await Promise.all(tasks);
        } catch (err) {
            console.log(`Failed to process batch: ${err}`);
        }
    }

    await browser.close();
}


async function main() {
    const keywords = ["restaurants"];
    const concurrencyLimit = 4;
    const pages = 5;
    const location = "uk";
    const retries = 3;
    const aggregateFiles = [];

    for (const keyword of keywords) {
        console.log("Crawl starting");
        await startScrape(keyword, pages, location, concurrencyLimit, retries);
        console.log("Crawl complete");
        aggregateFiles.push(`${keyword.replace(" ", "-")}.csv`);
    }

}


main();