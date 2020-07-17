import json
import os

from functools import reduce
import singer
from singer import metadata

LOGGER = singer.get_logger()

REPORT_STREAMS = {
    "common": {
        "default_dimension_fields":
        ["entityId", "advertiserId"],
        "fields": [
            "totalCost", "supplyCost", "amazonAudienceFee",
            "advertiserTimezone", "advertiserCountry", "amazonPlatformFee",
            "impressions", "clickThroughs", "CTR", "eCPM", "eCPC", "dpv14d",
            "dpvViews14d", "dpvClicks14d", "dpvr14d", "eCPDPV14d", "pRPV14d",
            "pRPVViews14d", "pRPVClicks14d", "pRPVr14d", "eCPPRPV14d",
            "atl14d", "atlViews14d", "atlClicks14d", "atlr14d", "eCPAtl14d",
            "atc14d", "atcViews14d", "atcClicks14d", "atcr14d", "eCPAtc14d",
            "purchases14d", "purchasesViews14d", "purchasesClicks14d",
            "purchaseRate14d", "eCPP14d", "newToBrandPurchases14d",
            "newToBrandPurchasesViews14d", "newToBrandPurchasesClicks14d",
            "newToBrandPurchaseRate14d", "newToBrandECPP14d",
            "percentOfPurchasesNewToBrand14d", "addToWatchlist14d",
            "addToWatchlistViews14d", "addToWatchlistClicks14d",
            "addToWatchlistCVR14d", "addToWatchlistCPA14d",
            "downloadedVideoPlays14d", "downloadedVideoPlaysViews14d",
            "downloadedVideoPlaysClicks14d", "downloadedVideoPlayRate14d",
            "eCPDVP14d", "videoStreams14d", "videoStreamsViews14d",
            "videoStreamsClicks14d", "videoStreamsRate14d", "eCPVS14d",
            "playTrailers14d", "playTrailersViews14d",
            "playerTrailersClicks14d", "playTrailerRate14d", "eCPPT14d",
            "rentals14d", "rentalsViews14d", "rentalsClicks14d",
            "rentalRate14d", "ecpr14d", "videoDownloads14d",
            "videoDownloadsViews14d", "videoDownloadsClicks14d",
            "videoDownloadRate14d", "ecpvd14d", "newSubscribeAndSave14d",
            "newSubscribeAndSaveViews14d", "newSubscribeAndSaveClicks14d",
            "newSubscribeAndSaveRate14d", "eCPnewSubscribeAndSave14d",
            "totalPixel14d", "totalPixelViews14d", "totalPixelClicks14d",
            "totalPixelCVR14d", "totalPixelCPA14d", "marketingLandingPage14d",
            "marketingLandingPageViews14d", "marketingLandingPageClicks14d",
            "marketingLandingPageCVR14d", "marketingLandingPageCPA14d",
            "subscriptionPage14d", "subscriptionPageViews14d",
            "subscriptionPageClicks14d", "subscriptionPageCVR14d",
            "subscriptionPageCPA14d", "signUpPage14d", "signUpPageViews14d",
            "signUpPageClicks14d", "signUpPageCVR14d", "signUpPageCPA14d",
            "application14d", "applicationViews14d", "applicationClicks14d",
            "applicationCVR14d", "applicationCPA14d", "gameLoad14d",
            "gameLoadViews14d", "gameLoadClicks14d", "gameLoadCVR14d",
            "gameLoadCPA14d", "widgetLoad14d", "widgetLoadViews14d",
            "widgetLoadClicks14d", "widgetLoadCVR14d", "widgetLoadCPA14d",
            "surveyStart14d", "surveyStartViews14d", "surveyStartClicks14d",
            "surveyStartCVR14d", "surveyStartCPA14d", "surveyFinish14d",
            "surveyFinishViews14d", "surveyFinishClicks14d",
            "surveyFinishCVR14d", "surveyFinishCPA14d", "bannerInteraction14d",
            "bannerInteractionViews14d", "bannerInteractionClicks14d",
            "bannerInteractionCVR14d", "bannerInteractionCPA14d",
            "widgetInteraction14d", "widgetInteractionViews14d",
            "widgetInteractionClicks14d", "widgetInteractionCVR14d",
            "widgetInteractionCPA14d", "gameInteraction14d",
            "gameInteractionViews14d", "gameInteractionClicks14d",
            "gameInteractionCVR14d", "gameInteractionCPA14d", "emailLoad14d",
            "emailLoadViews14d", "emailLoadClicks14d", "emailLoadCVR14d",
            "emailLoadCPA14d", "emailInteraction14d",
            "emailInteractionViews14d", "emailInteractionClicks14d",
            "emailInteractionCVR14d", "emailInteractionCPA14d",
            "submitButton14d", "submitButtonViews14d", "submitButtonClicks14d",
            "submitButtonCVR14d", "submitButtonCPA14d", "purchaseButton14d",
            "purchaseButtonViews14d", "purchaseButtonClicks14d",
            "purchaseButtonCVR14d", "purchaseButtonCPA14d",
            "clickOnRedirect14d", "clickOnRedirectViews14d",
            "clickOnRedirectClicks14d", "clickOnRedirectCVR14d",
            "clickOnRedirectCPA14d", "signUpButton14d", "signUpButtonViews14d",
            "signUpButtonClicks14d", "signUpButtonCVR14d",
            "signUpButtonCPA14d", "subscriptionButton14d",
            "subscriptionButtonViews14d", "subscriptionButtonClicks14d",
            "subscriptionButtonCVR14d", "subscriptionButtonCPA14d",
            "successPage14d", "successPageViews14d", "successPageClicks14d",
            "successPageCVR14d", "successPageCPA14d", "thankYouPage14d",
            "thankYouPageViews14d", "thankYouPageClicks14d",
            "thankYouPageCVR14d", "thankYouPageCPA14d", "registrationForm14d",
            "registrationFormViews14d", "registrationFormClicks14d",
            "registrationFormCVR14d", "registrationFormCPA14d",
            "registrationConfirmPage14d", "registrationConfirmPageViews14d",
            "registrationConfirmPageClicks14d",
            "registrationConfirmPageCVR14d", "registrationConfirmPageCPA14d",
            "storeLocatorPage14d", "storeLocatorPageViews14d",
            "storeLocatorPageClicks14d", "storeLocatorPageCVR14d",
            "storeLocatorPageCPA14d", "mobileAppFirstStarts14d",
            "mobileAppFirstStartViews14d", "mobileAppFirstStartClicks14d",
            "mobileAppFirstStartCVR14d", "mobileAppFirstStartsCPA14d",
            "brandStoreEngagement1", "brandStoreEngagement1Views",
            "brandStoreEngagement1Clicks", "brandStoreEngagement1CVR",
            "brandStoreEngagement1CPA", "brandStoreEngagement2",
            "brandStoreEngagement2Views", "brandStoreEngagement2Clicks",
            "brandStoreEngagement2CVR", "brandStoreEngagement2CPA",
            "brandStoreEngagement3", "brandStoreEngagement3Views",
            "brandStoreEngagement3Clicks", "brandStoreEngagement3CVR",
            "brandStoreEngagement3CPA", "brandStoreEngagement4",
            "brandStoreEngagement4Views", "brandStoreEngagement4Clicks",
            "brandStoreEngagement4CVR", "brandStoreEngagement4CPA",
            "brandStoreEngagement5", "brandStoreEngagement5Views",
            "brandStoreEngagement5Clicks", "brandStoreEngagement5CVR",
            "brandStoreEngagement5CPA", "brandStoreEngagement6",
            "brandStoreEngagement6Views", "brandStoreEngagement6Clicks",
            "brandStoreEngagement6CVR", "brandStoreEngagement6CPA",
            "brandStoreEngagement7", "brandStoreEngagement7Views",
            "brandStoreEngagement7Clicks", "brandStoreEngagement7CVR",
            "brandStoreEngagement7CPA", "addedToShoppingCart14d",
            "addedToShoppingCartViews14d", "addedToShoppingCartClicks14d",
            "addedToShoppingCartCVR14d", "addedToShoppingCartCPA14d",
            "productPurchased", "productPurchasedViews",
            "productPurchasedClicks", "productPurchasedCVR",
            "productPurchasedCPA", "homepageVisit14d", "homepageVisitViews14d",
            "homepageVisitClicks14d", "homepageVisitCVR14d",
            "homepageVisitCPA14d", "videoStarted", "videoStartedViews",
            "videoStartedClicks", "videoStartedCVR", "videoStartedCPA",
            "videoCompleted", "videoCompletedViews", "videoEndClicks",
            "videoCompletedCVR", "videoCompletedCPA", "messageSent14d",
            "messageSentViews14d", "messageSentClicks14d", "messageSentCVR14d",
            "messageSentCPA14d", "mashupClickToPage", "mashupClickToPageViews",
            "mashupClickToPageClicks", "mashupClickToPageCVR",
            "mashupClickToPageCPA", "mashupBackupImage",
            "mashupBackupImageViews", "mashupBackupImageClicks",
            "mashupBackupImageCVR", "mashupBackupImageCPA",
            "mashupAddToCart14d", "mashupAddToCartViews14d",
            "mashupAddToCartClicks14d", "mashupAddToCartClickCVR14d",
            "mashupAddToCartCPA14d", "mashupAddToWishlist14d",
            "mashupAddToWishlistViews14d", "mashupAddToWishlistClicks14d",
            "mashupAddToWishlistCVR14d", "mashupAddToWishlistCPA14d",
            "mashupSubscribeAndSave14d", "mashupSubscribeAndSaveClickViews14d",
            "mashupSubscribeAndSaveClick14d", "mashupSubscribeAndSaveCVR14d",
            "mashupSubscribeAndSaveCPA14d", "mashupClipCouponClick14d",
            "mashupClipCouponClickViews14d", "mashupClipCouponClickClicks14d",
            "mashupClipCouponClickCVR14d", "mashupClipCouponClickCPA14d",
            "mashupShopNowClick14d", "mashupShopNowClickViews14d",
            "mashupShopNowClickClicks14d", "mashupShopNowClickCVR14d",
            "mashupShopNowClickCPA14d", "referral14d", "referralViews14d",
            "referralClicks14d", "referralCVR14d", "referralCPA14d",
            "accept14d", "acceptViews14d", "acceptClicks14d", "acceptCVR14d",
            "acceptCPA14d", "decline14d", "declineViews14d",
            "declineClicks14d", "declineCVR14d", "declineCPA14d", "videoStart",
            "videoFirstQuartile", "videoMidpoint", "videoThirdQuartile",
            "videoComplete", "videoCompletionRate", "ecpvc", "videoPause",
            "videoResume", "videoMute", "videoUnmute", "dropDownSelection14d",
            "dropDownSelectionViews14d", "dropDownSelectionClicks14d",
            "dropDownSelectionCVR14d", "dropDownSelectionCPA14d"
        ]
    },
    "campaign": {
        "replication_key": "date",
        "default_dimension_fields": ["date"],
        "dimensions": ["ORDER", "LINE_ITEM", "CREATIVE"],
        "timeUnit": "DAILY",
        "fields": [
            "agencyFee", "totalFee", "3pFeeAutomotive",
            "3pFeeAutomotiveAbsorbed", "3pFeeComScore",
            "3pFeeComScoreAbsorbed", "3pFeeCPM1", "3pFeeCPM1Absorbed",
            "3pFeeCPM2", "3pFeeCPM2Absorbed", "3pFeeCPM3", "3pFeeCPM3Absorbed",
            "3pFeeDoubleclickCampaignManager",
            "3pFeeDoubleclickCampaignManagerAbsorbed", "3pFeeDoubleVerify",
            "3pFeeDoubleVerifyAbsorbed", "3pFeeIntegralAdScience",
            "3pFeeIntegralAdScienceAbsorbed", "3PFees", "unitsSold14d",
            "sales14d", "ROAS14d", "eRPM14d", "newToBrandUnitsSold14d",
            "newToBrandProductSales14d", "newToBrandROAS14d",
            "newToBrandERPM14d", "totalPRPV14d", "totalPRPVViews14d",
            "totalPRPVClicks14d", "totalPRPVr14d", "totalECPPRPV14d",
            "totalPurchases14d", "totalPurchasesViews14d",
            "totalPurchasesClicks14d", "totalPurchaseRate14d", "totalECPP14d",
            "totalNewToBrandPurchases14d", "totalNewToBrandPurchasesViews14d",
            "totalNewToBrandPurchasesClicks14d",
            "totalNewToBrandPurchaseRate14d", "totalNewToBrandECPP14d",
            "totalPercentOfPurchasesNewToBrand14d", "totalUnitsSold14d",
            "totalSales14d", "totalROAS14d", "totalERPM14d",
            "totalNewToBrandUnitsSold14d", "totalNewToBrandProductSales14d",
            "totalNewToBrandROAS14d", "totalNewToBrandERPM14d",
            "viewableImpressions", "measurableImpressions", "measurableRate",
            "viewabilityRate", "totalDetailPageViews14d",
            "totalDetailPageViewViews14d", "totalDetailPageClicks14d",
            "totalDetailPageViewsCVR14d", "totalDetaiPageViewCPA14d",
            "totalAddToList14d", "totalAddToListViews14d",
            "totalAddToListClicks14d", "totalAddToListCVR14d",
            "totalAddToListCPA14d", "totalAddToCart14d",
            "totalAddToCartViews14d", "totalAddToCartClicks14d",
            "totalAddToCartCVR14d", "totalAddToCartCPA14d",
            "totalSubscribeAndSaveSubscriptions14d",
            "totalSubscribeAndSaveSubscriptionViews14d",
            "totalSubscribeAndSaveSubscriptionClicks14d",
            "totalSubscribeAndSaveSubscriptionCVR14d",
            "totalSubscribeAndSaveSubscriptionCPA14d"
        ]
    },
    "inventory": {
        "replication_key": "date",
        "default_dimension_fields": ["date", "placementSize", "placementName"],
        "dimensions": ["ORDER", "LINE_ITEM", "SITE", "SUPPLY"],
        "timeUnit": "DAILY",
        "fields": [
            "agencyFee", "totalFee", "3pFeeAutomotive",
            "3pFeeAutomotiveAbsorbed", "3pFeeComScore",
            "3pFeeComScoreAbsorbed", "3pFeeCPM1", "3pFeeCPM1Absorbed",
            "3pFeeCPM2", "3pFeeCPM2Absorbed", "3pFeeCPM3", "3pFeeCPM3Absorbed",
            "3pFeeDoubleclickCampaignManager",
            "3pFeeDoubleclickCampaignManagerAbsorbed", "3pFeeDoubleVerify",
            "3pFeeDoubleVerifyAbsorbed", "3pFeeIntegralAdScience",
            "3pFeeIntegralAdScienceAbsorbed", "3PFees", "unitsSold14d",
            "sales14d", "ROAS14d", "eRPM14d", "newToBrandUnitsSold14d",
            "newToBrandProductSales14d", "newToBrandROAS14d",
            "newToBrandERPM14d", "totalPRPV14d", "totalPRPVViews14d",
            "totalPRPVClicks14d", "totalPRPVr14d", "totalECPPRPV14d",
            "totalPurchases14d", "totalPurchasesViews14d",
            "totalPurchasesClicks14d", "totalPurchaseRate14d", "totalECPP14d",
            "totalNewToBrandPurchases14d", "totalNewToBrandPurchasesViews14d",
            "totalNewToBrandPurchasesClicks14d",
            "totalNewToBrandPurchaseRate14d", "totalNewToBrandECPP14d",
            "totalPercentOfPurchasesNewToBrand14d", "totalUnitsSold14d",
            "totalSales14d", "totalROAS14d", "totalERPM14d",
            "totalNewToBrandUnitsSold14d", "totalNewToBrandProductSales14d",
            "totalNewToBrandROAS14d", "totalNewToBrandERPM14d",
            "viewableImpressions", "measurableImpressions", "measurableRate",
            "viewabilityRate", "totalDetailPageViews14d",
            "totalDetailPageViewViews14d", "totalDetailPageClicks14d",
            "totalDetailPageViewsCVR14d", "totalDetaiPageViewCPA14d",
            "totalAddToList14d", "totalAddToListViews14d",
            "totalAddToListClicks14d", "totalAddToListCVR14d",
            "totalAddToListCPA14d", "totalAddToCart14d",
            "totalAddToCartViews14d", "totalAddToCartClicks14d",
            "totalAddToCartCVR14d", "totalAddToCartCPA14d",
            "totalSubscribeAndSaveSubscriptions14d",
            "totalSubscribeAndSaveSubscriptionViews14d",
            "totalSubscribeAndSaveSubscriptionClicks14d",
            "totalSubscribeAndSaveSubscriptionCVR14d",
            "totalSubscribeAndSaveSubscriptionCPA14d", "placementName",
            "placementSize"
        ]
    },
    "audience": {
        "replication_key": "intervalStart",
        "default_dimension_fields":
        ["intervalStart", "intervalEnd", "segment"],
        "dimensions": ["ORDER", "LINE_ITEM"],
        "timeUnit": "SUMMARY",
        "fields": [
            "segmentClassCode", "segmentSource", "segmentType",
            "segmentMarketplaceID", "targetingMethod"
        ]
    }
}

DIMENSION_FIELDS = [
    "lineItemBudget", "orderId", "lineItemExternalId", "orderStartDate",
    "orderBudget", "lineItemStartDate", "date", "siteName", "orderExternalId",
    "orderEndDate", "supplySourceName", "lineItemName", "lineItemId",
    "reportDate", "orderName", "orderCurrency", "advertiserName",
    "__sdc_record_hash", "advertiserId", "lineItemEndDate", "entityId",
    "reportDate", "segment", "intervalEnd", "lineitemtype", "intervalStart",
    "segmentMarketplaceId", "creativeSize", "creativeID", "creativeName",
    "creativeType"
]

DIMENSION_PRIMARY_KEYS = {
    "common": {
        "primary_keys": ["entityId", "advertiserId"]
    },
    "ORDER": {
        "fields": [
            "orderName", "orderId", "orderStartDate", "orderEndDate",
            "orderBudget", "orderExternalId", "orderCurrency"
        ],
        "primary_keys": ["orderId"]
    },
    "LINE_ITEM": {
        "fields": [
            "lineItemName", "lineItemId", "lineItemStartDate",
            "lineItemEndDate", "lineItemBudget", "lineItemExternalId"
        ],
        "primary_keys": ["lineItemId"]
    },
    "CREATIVE": {
        "fields": ["creativeName", "creativeID", "creativeType", "creativeSize"],
        "primary_keys": ["creativeID"]
    },
    "SITE": {
        "fields": ["siteName"],
        "primary_keys": ["siteName"]
    },
    "SUPPLY": {
        "fields": ["supplySourceName"],
        "primary_keys": ["supplySourceName"]
    }
}

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [
        f for f in os.listdir(shared_schemas_path)
        if os.path.isfile(os.path.join(shared_schemas_path, f))
    ]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[shared_file] = json.load(data_file)

    return shared_schema_refs


def resolve_schema_references(schema, refs):
    if '$ref' in schema['properties']:
        link = schema['properties']['$ref']
        schema['properties'].update(refs[link]['properties'])
        del schema['properties']['$ref']


def fields_for_report_dimensions(report_type, report_dimensions):
    report_primary_keys = []
    # All reports contains default
    for field in REPORT_STREAMS['common'][
            'default_dimension_fields']:
        report_primary_keys.append(field)
    for dimension in report_dimensions:
        report_primary_keys.extend(DIMENSION_PRIMARY_KEYS.get(dimension).get('fields'))
    for field in REPORT_STREAMS[report_type][
            'default_dimension_fields']:
        report_primary_keys.append(field)

    if report_type == 'audience':
        report_primary_keys.append('lineitemtype')

    report_primary_keys.sort()
    return report_primary_keys

def get_report_dimensions(report):
    if report.get('dimensions'):
        return report.get('dimensions')
    return REPORT_STREAMS.get(report.get('type')).get(
            'dimensions')

def get_schemas(reports):
    schemas = {}
    field_metadata = {}

    refs = load_shared_schema_refs()

    # JSON schemas for each report
    for report in reports:
        report_name = report.get('name')
        report_type = report.get('type')
        report_dimensions = get_report_dimensions(report)
        replication_key = REPORT_STREAMS.get(report_type).get(
            'replication_key')

        report_path = get_abs_path(f'schemas/{report_type.lower()}.json')

        with open(report_path) as file:
            schema = json.load(file)

        resolve_schema_references(schema, refs)

        schemas[report_name] = schema
        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=['__sdc_record_hash'],
            valid_replication_keys=[replication_key],
            replication_method='INCREMENTAL')
        mdata = metadata.to_map(mdata)
        mdata = reduce(
            lambda mdata, field_name: metadata.write(mdata, (
                "properties", field_name), "inclusion", "automatic"),
            fields_for_report_dimensions(report_type, report_dimensions),
            mdata)

        field_metadata[report_name] = mdata

    return schemas, field_metadata
