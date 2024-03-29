package com.machinedoll.projectdemo.schema

case class FileInfo(name: String, path: String, created: String) extends Serializable

case class GDELTReferenceLink(size: Double, hash: String, url: String, time: String) extends Serializable

case class GDELTRawData(GLOBALEVENTID: Int,
                        SQLDATE: Int,
                        MonthYear: Int,
                        Year: Int,
                        FractionDate: Float,
                        Actor1Code: String,
                        Actor1Name: String,
                        Actor1CountryCode: String,
                        Actor1KnownGroupCode: String,
                        Actor1EthnicCode: String,
                        Actor1Religion1Code: String,
                        Actor1Religion2Code: String,
                        Actor1Type1Code: String,
                        Actor1Type2Code: String,
                        Actor1Type3Code: String,
                        Actor2Code: String,
                        Actor2Name: String,
                        Actor2CountryCode: String,
                        Actor2KnownGroupCode: String,
                        Actor2EthnicCode: String,
                        Actor2Religion1Code: String,
                        Actor2Religion2Code: String,
                        Actor2Type1Code: String,
                        Actor2Type2Code: String,
                        Actor2Type3Code: String,
                        IsRootEvent: Int,
                        EventCode: String,
                        EventBaseCode: String,
                        EventRootCode: String,
                        QuadClass: Int,
                        GoldsteinScale: Float,
                        NumMentions: Int,
                        NumSources: Int,
                        NumArticles: Int,
                        AvgTone: Float,
                        Actor1Geo_Type: Int,
                        Actor1Geo_FullName: String,
                        Actor1Geo_CountryCode: String,
                        Actor1Geo_ADM1Code: String,
                        Actor1Geo_ADM2Code: String,
                        Actor1Geo_Lat: Float,
                        Actor1Geo_Long: Float,
                        Actor1Geo_FeatureID: String,
                        Actor2Geo_Type: Int,
                        Actor2Geo_FullName: String,
                        Actor2Geo_CountryCode: String,
                        Actor2Geo_ADM1Code: String,
                        Actor2Geo_ADM2Code: String,
                        Actor2Geo_Lat: Float,
                        Actor2Geo_Long: Float,
                        Actor2Geo_FeatureID: String,
                        ActionGeo_Type: Int,
                        ActionGeo_FullName: String,
                        ActionGeo_CountryCode: String,
                        ActionGeo_ADM1Code: String,
                        ActionGeo_ADM2Code: String,
                        ActionGeo_Lat: Float,
                        ActionGeo_Long: Float,
                        ActionGeo_FeatureID: String,
                        DATEADDED: Int,
                        SOURCEURL: String)

case class Export(GLOBALEVENTID: Option[Int],                   //1
                        SQLDATE: Option[Int],
                        MonthYear: Option[Int],
                        Year: Option[Int],
                        FractionDate: Option[Float],
                        Actor1Code: Option[String],
                        Actor1Name: Option[String],
                        Actor1CountryCode: Option[String],
                        Actor1KnownGroupCode: Option[String],
                        Actor1EthnicCode: Option[String],
                        Actor1Religion1Code: Option[String],
                        Actor1Religion2Code: Option[String],
                        Actor1Type1Code: Option[String],
                        Actor1Type2Code: Option[String],
                        Actor1Type3Code: Option[String],
                        Actor2Code: Option[String],
                        Actor2Name: Option[String],
                        Actor2CountryCode: Option[String],
                        Actor2KnownGroupCode: Option[String],
                        Actor2EthnicCode: Option[String],       //20
                        Actor2Religion1Code: Option[String],
                        Actor2Religion2Code: Option[String],
                        Actor2Type1Code: Option[String],
                        Actor2Type2Code: Option[String],
                        Actor2Type3Code: Option[String],
                        IsRootEvent: Option[Int],
                        EventCode: Option[String],
                        EventBaseCode: Option[String],
                        EventRootCode: Option[String],
                        QuadClass: Option[Int],                 //30
                        GoldsteinScale: Option[Float],
                        NumMentions: Option[Int],
                        NumSources: Option[Int],
                        NumArticles: Option[Int],
                        AvgTone: Option[Float],
                        Actor1Geo_Type: Option[Int],
                        Actor1Geo_FullName: Option[String],
                        Actor1Geo_CountryCode: Option[String],
                        Actor1Geo_ADM1Code: Option[String],
                        Actor1Geo_ADM2Code: Option[String],     //40
                        Actor1Geo_Lat: Option[Float],
                        Actor1Geo_Long: Option[Float],
                        Actor1Geo_FeatureID: Option[String],
                        Actor2Geo_Type: Option[Int],
                        Actor2Geo_FullName: Option[String],
                        Actor2Geo_CountryCode: Option[String],
                        Actor2Geo_ADM1Code: Option[String],
                        Actor2Geo_ADM2Code: Option[String],
                        Actor2Geo_Lat: Option[Float],
                        Actor2Geo_Long: Option[Float],          //50
                        Actor2Geo_FeatureID: Option[String],
                        ActionGeo_Type: Option[Int],
                        ActionGeo_FullName: Option[String],
                        ActionGeo_CountryCode: Option[String],
                        ActionGeo_ADM1Code: Option[String],
                        ActionGeo_ADM2Code: Option[String],
                        ActionGeo_Lat: Option[Float],
                        ActionGeo_Long: Option[Float],
                        ActionGeo_FeatureID: Option[String],
                        DATEADDED: Option[Int],
                        SOURCEURL: Option[String]
                 )extends Serializable
//case class Export(x: Option[Int])