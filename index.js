const fetch = require('node-fetch');
const fs = require('fs');

// You can (and probably should) override this here or in the counties file. All counties results in about 49,233 brokers!
const counties = require('./counties');


// Decide which details to get from google
// https://developers.google.com/maps/documentation/places/web-service/details#fields
const detailsFields = ['name', 'website', 'formatted_phone_number']

// Output file
const output_location = 'places'

// Google Places API configurations
const googleApiKey =  'YOUR_GOOGLE_MAPS_API_KEY'
const googleBaseUrl = 'https://maps.googleapis.com/maps/api/place';
const findPlaceEndpoint = 'findplacefromtext/json';
const detailsEndpoint = 'details/json';
const inputType = 'textquery';
const placeFields = 'name,place_id';

// New York brokers API endpoint
const nyBrokersUrl = 'https://data.ny.gov/resource/9twf-9yig.json';

// Fetch broker data for counties in `counties` list
async function fetchDataByCounty() {
  const response = await fetch(nyBrokersUrl);
  const data = await response.json();
  const filteredData = data.filter(record => counties.includes(record.county));
  return filteredData;
}

// Fetch details of a place using Google Places API
async function fetchPlaceDetails(address) {
  const {
    business_name: name,
    business_address_1: street,
    business_city: city,
    county,
    business_state: state,
    business_zip: postalCode,
  } = address;
  
  const query = `${name} ${street} ${city} ${county} ${state} United States ${postalCode}`;
  const url = `${googleBaseUrl}/${findPlaceEndpoint}?input=${query}&fields=${placeFields}&inputtype=${inputType}&key=${googleApiKey}`;
  const response = await fetch(url);
  
  const data = await response.json();
  console.log(data)
  const placeId = data.candidates?.[0]?.place_id;

  if (!placeId) {
    return address;
  }

  const detailsUrl = `${googleBaseUrl}/${detailsEndpoint}?place_id=${placeId}&fields=${detailsFields.join(',')}&key=${googleApiKey}`;
  const detailsResponse = await fetch(detailsUrl);
  const detailsData = await detailsResponse.json();
  
  const mergedData = { ...address, ...detailsData.result, ...data.candidates[0] };
  return mergedData;
}

// Fetch details for all places and write data to file
async function fetchAllDetails() {
  const addresses = await fetchDataByCounty();
  const detailsPromises = addresses.map(fetchPlaceDetails);
  const details = await Promise.all(detailsPromises);
  return details;
}

async function writeToFile(data) {
  const fileName = output_location + '.json';
  fs.writeFile(fileName, JSON.stringify(data, null, 2), (err) => {
    if (err) {
      console.error(err);
    } else {
      console.log(`Data written to ${fileName}`);
    }
  });
}

console.log(`Fetching details for all brokers in the following counties: ${counties.join(',')}`)

fetchAllDetails()
  .then((data) => {
    writeToFile(data);
  })
  .catch((error) => {
    console.error(error);
  });
