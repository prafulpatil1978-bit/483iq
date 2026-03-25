# 483IQ — FDA Inspection Intelligence

[![Netlify Status](https://api.netlify.com/api/v1/badges/f17faa07-a741-4238-8694-1b4ffa753b79/deploy-status)](https://app.netlify.com/sites/483iq/deploys)

A single-page application for browsing and analyzing FDA Form 483 inspection data.

**Live app:** [https://483iq.netlify.app](https://483iq.netlify.app)

## Deploy

This repo is connected to Netlify. Every push to `main` triggers an automatic deploy.  
Pull requests automatically generate deploy previews.

## ETL Pipeline

FDA inspection data is refreshed weekly via GitHub Actions.  
See `.github/workflows/etl-weekly.yml` for the schedule.
