-- MySQL dump 10.13  Distrib 9.0.1, for Win64 (x86_64)
--
-- Host: localhost    Database: rank_country_internet_speed
-- ------------------------------------------------------
-- Server version	9.0.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `rank_country_broadband`
--

DROP TABLE IF EXISTS `rank_country_broadband`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rank_country_broadband` (
  `Ranking` text,
  `Change_in_Ranking` text,
  `Country` text,
  `Mbps` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rank_country_broadband`
--

LOCK TABLES `rank_country_broadband` WRITE;
/*!40000 ALTER TABLE `rank_country_broadband` DISABLE KEYS */;
INSERT INTO `rank_country_broadband` VALUES ('1','-','Singapore','285.02'),('2','-','United Arab Emirates','283.34'),('3','-','Hong Kong (SAR)','271.05'),('4','-','Chile','263.32'),('5','-','United States','244.68'),('6','-','Iceland','242.03'),('7','-','France','232.30'),('8','-','Thailand','230.98'),('9','-','Denmark','224.82'),('10','-','China','223.57'),('11','-','Spain','211.56'),('12','-','Romania','207.04'),('13','-','Switzerland','202.74'),('14','-','Israel','201.60'),('15','-','Japan','193.54'),('16','-','Canada','191.54'),('17','-','Taiwan','190.32'),('18','-','Netherlands','190.13'),('19','-','Liechtenstein','187.79'),('20','-','Hungary','180.90'),('21','-','New Zealand','177.96'),('22','-','Portugal','177.00'),('23','-','Kuwait','172.00'),('24','-','Brazil','160.82'),('25','-','Macau (SAR)','159.63'),('26','-','Uruguay','156.75'),('27','-','Qatar','155.19'),('28','-','Poland','154.66'),('29','-','Peru','151.09'),('30','-','Panama','150.15'),('31','-','South Korea','150.08'),('32','-','Sweden','149.84'),('33','-','Luxembourg','146.47'),('34','-','Jordan','143.86'),('35','-','Colombia','140.17'),('36','-','Norway','138.61'),('37','-','Vietnam','136.06'),('38','-','Malaysia','132.67'),('39','-','Moldova','130.46'),('40','-','Ireland','126.31'),('41','-','Lithuania','121.27'),('42','-','Trinidad and Tobago','119.13'),('43','-','Finland','116.09'),('44','-','Malta','115.44'),('45','-','Saudi Arabia','112.44'),('46','-','United Kingdom','106.51'),('47','-','Austria','99.72'),('48','-','Belgium','96.97'),('49','-','San Marino','94.78'),('50','-','Philippines','94.42'),('51','-','Costa Rica','94.33'),('52','-','Barbados','94.25'),('53','-','Latvia','93.56'),('54','-','Slovenia','93.37'),('55','-','Ecuador','93.32'),('56','-','Paraguay','91.44'),('57','-','Germany','90.50'),('58','-','Grenada','89.70'),('59','-','Dominica','88.97'),('60','-','Russia','87.08'),('61','-','Cyprus','86.61'),('62','-','Argentina','84.76'),('63','-','Bahrain','84.51'),('64','-','Estonia','84.39'),('65','-','Slovakia','84.31'),('66','-','Guyana','83.41'),('67','-','Serbia','83.09'),('68','-','Italy','81.40'),('69','-','Bulgaria','80.76'),('70','-','Ukraine','80.20'),('71','-','Egypt','80.00'),('72','-','Brunei','77.50'),('73','-','Jamaica','75.96'),('74','-','Montenegro','74.06'),('75','-','Mexico','72.77'),('76','-','Kosovo','71.76'),('77','-','Nepal','71.72'),('78','-','Mongolia','71.58'),('79','-','Oman','71.44'),('80','-','Czechia','70.34'),('81','-','Palestine','67.36'),('82','-','Australia','66.58'),('83','-','Uzbekistan','65.42'),('84','-','Côte d\'Ivoire','64.93'),('85','-','India','63.99'),('86','-','Saint Kitts and Nevis','63.96'),('87','-','Albania','63.61'),('88','-','Kyrgyzstan','63.57'),('89','-','Croatia','62.95'),('90','-','Belarus','62.14'),('91','-','The Bahamas','61.91'),('92','-','Armenia','56.42'),('93','-','Nicaragua','55.90'),('94','-','Kazakhstan','53.86'),('95','-','Venezuela','53.81'),('96','-','El Salvador','52.82'),('97','-','Mauritius','51.82'),('98','-','Guatemala','51.21'),('99','-','Greece','49.40'),('100','-','Bangladesh','47.43'),('101','-','South Africa','47.28'),('102','-','Honduras','47.18'),('103','-','Belize','47.07'),('104','-','Seychelles','45.88'),('105','-','Cambodia','45.39'),('106','-','North Macedonia','44.20'),('107','-','Burkina Faso','42.59'),('108','-','Turkey','42.43'),('109','-','Congo','41.64'),('110','-','Gabon','40.66'),('111','-','DR Congo','40.64'),('112','-','Antigua and Barbuda','40.24'),('113','-','Azerbaijan','39.87'),('114','-','Ghana','37.89'),('115','-','Laos','37.55'),('116','-','Haiti','35.96'),('117','-','Madagascar','35.77'),('118','-','Dominican Republic','35.38'),('119','-','Iraq','34.03'),('120','-','Rwanda','33.73'),('121','-','Bolivia','33.19'),('122','-','Bosnia and Herzegovina','32.54'),('123','-','Tajikistan','31.72'),('124','-','Morocco','31.43'),('125','-','Indonesia','31.38'),('126','-','Togo','31.11'),('127','-','Georgia','26.70'),('128','-','Mauritania','25.33'),('129','-','Benin','24.23'),('130','-','Senegal','23.99'),('131','-','Sri Lanka','22.83'),('132','-','Nigeria','22.47'),('133','-','Myanmar (Burma)','21.15'),('134','-','Fiji','19.99'),('135','-','Angola','19.47'),('136','-','Zambia','19.31'),('137','-','Tanzania','18.84'),('138','-','Lebanon','16.18'),('139','-','Zimbabwe','15.94'),('140','-','Somalia','15.90'),('141','-','Mozambique','15.85'),('142','-','Pakistan','15.26'),('143','-','Uganda','15.21'),('144','-','Iran','14.77'),('145','-','Algeria','14.24'),('146','-','Namibia','13.90'),('147','-','Maldives','13.48'),('148','-','Suriname','12.45'),('149','-','Kenya','11.39'),('150','-','Libya','11.19'),('151','-','Tunisia','10.16'),('152','-','Cameroon','9.92'),('153','-','Ethiopia','9.56'),('154','-','Guinea','9.41'),('155','-','Botswana','8.13'),('156','-','Yemen','7.30'),('157','-','Syria','4.77'),('158','-','Afghanistan','3.56'),('159','-','Cuba','2.79');
/*!40000 ALTER TABLE `rank_country_broadband` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `rank_country_mobile`
--

DROP TABLE IF EXISTS `rank_country_mobile`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `rank_country_mobile` (
  `Ranking` text,
  `Change_in_Ranking` text,
  `Country` text,
  `Mbps` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `rank_country_mobile`
--

LOCK TABLES `rank_country_mobile` WRITE;
/*!40000 ALTER TABLE `rank_country_mobile` DISABLE KEYS */;
INSERT INTO `rank_country_mobile` VALUES ('1','-','Qatar','334.63'),('2','-','United Arab Emirates','323.61'),('3','-','Kuwait','226.56'),('4','-','Norway','145.19'),('5','-','Denmark','144.93'),('6','-','South Korea','139.04'),('7','-','China','135.71'),('8','-','Saudi Arabia','128.03'),('9','-','Netherlands','120.96'),('10','-','Bahrain','113.87'),('11','-','United States','113.10'),('12','-','India','107.03'),('13','-','Singapore','104.98'),('14','-','France','103.40'),('15','-','Finland','103.23'),('16','-','Bulgaria','102.35'),('17','-','Lithuania','100.38'),('18','-','Sweden','100.08'),('19','-','Malaysia','98.84'),('20','-','Croatia','96.95'),('21','-','Estonia','96.09'),('22','-','Australia','94.28'),('23','-','Switzerland','91.25'),('24','-','North Macedonia','89.70'),('25','-','Czechia','87.27'),('26','-','Latvia','86.92'),('27','-','Canada','86.32'),('28','-','Portugal','85.02'),('29','-','Taiwan','84.51'),('30','-','Austria','83.97'),('31','-','Greece','82.11'),('32','-','Oman','80.19'),('33','-','Belgium','79.27'),('34','-','Slovenia','73.43'),('35','-','New Zealand','72.25'),('36','-','Cyprus','67.91'),('37','-','Brazil','63.73'),('38','-','Romania','61.64'),('39','-','Hong Kong (SAR)','61.36'),('40','-','Germany','57.68'),('41','-','Montenegro','57.36'),('42','-','Poland','56.97'),('43','-','Slovakia','54.75'),('44','-','United Kingdom','54.51'),('45','-','Hungary','54.44'),('46','-','Serbia','53.65'),('47','-','Italy','53.63'),('48','-','Vietnam','53.38'),('49','-','Azerbaijan','52.63'),('50','-','South Africa','52.30'),('51','-','Thailand','52.05'),('52','-','Albania','50.94'),('53','-','Spain','50.69'),('54','-','Kazakhstan','46.63'),('55','-','Japan','46.56'),('56','-','Morocco','44.04'),('57','-','Israel','43.24'),('58','-','Turkey','41.97'),('59','-','Ireland','41.11'),('60','-','Georgia','40.06'),('61','-','Chile','39.91'),('62','-','Kosovo','39.00'),('63','-','Moldova','38.99'),('64','-','Guatemala','37.08'),('65','-','Iran','36.97'),('66','-','Kyrgyzstan','36.73'),('67','-','Lebanon','34.98'),('68','-','Costa Rica','33.29'),('69','-','Philippines','33.19'),('70','-','Argentina','31.72'),('71','-','Honduras','31.45'),('72','-','Iraq','31.28'),('73','-','Mexico','30.91'),('74','-','Myanmar (Burma)','30.89'),('75','-','Armenia','30.63'),('76','-','Dominican Republic','30.52'),('77','-','Uzbekistan','30.34'),('78','-','El Salvador','29.99'),('79','-','Laos','28.92'),('80','-','Indonesia','28.35'),('81','-','Tanzania','27.94'),('82','-','Kenya','27.66'),('83','-','Cambodia','27.56'),('84','-','Jordan','27.16'),('85','-','Tunisia','27.13'),('86','-','Russia','25.63'),('87','-','Egypt','25.49'),('88','-','Algeria','24.91'),('89','-','Ecuador','24.67'),('90','-','Bangladesh','24.49'),('91','-','Bosnia and Herzegovina','22.29'),('92','-','Nigeria','21.96'),('93','-','Ukraine','21.24'),('94','-','Peru','20.56'),('95','-','Panama','19.32'),('96','-','Paraguay','18.94'),('97','-','Pakistan','18.67'),('98','-','Sri Lanka','18.53'),('99','-','Nicaragua','18.44'),('100','-','Colombia','18.40'),('101','-','Libya','15.36'),('102','-','Mongolia','14.96'),('103','-','Syria','14.07'),('104','-','Venezuela','12.66'),('105','-','Belarus','11.87'),('106','-','Bolivia','11.09'),('107','-','Yemen','6.38'),('108','-','Cuba','4.02');
/*!40000 ALTER TABLE `rank_country_mobile` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-08-02 21:08:10
