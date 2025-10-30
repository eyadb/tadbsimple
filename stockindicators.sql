-- phpMyAdmin SQL Dump
-- version 5.2.2
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Oct 30, 2025 at 09:27 PM
-- Server version: 11.8.3-MariaDB-log
-- PHP Version: 7.2.34

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `u360608955_YFskB`
--

-- --------------------------------------------------------

--
-- Table structure for table `stockindicators`
--

CREATE TABLE `stockindicators` (
  `symbol` varchar(10) NOT NULL,
  `date` date NOT NULL,
  `sma5` decimal(12,2) DEFAULT NULL,
  `sma10` decimal(12,2) DEFAULT NULL,
  `sma20` decimal(12,2) DEFAULT NULL,
  `sma50` decimal(12,2) DEFAULT NULL,
  `sma100` decimal(12,2) DEFAULT NULL,
  `sma200` decimal(12,2) DEFAULT NULL,
  `sma5s1` decimal(12,2) DEFAULT NULL,
  `sma10s1` decimal(12,2) DEFAULT NULL,
  `sma20s1` decimal(12,2) DEFAULT NULL,
  `sma50s1` decimal(12,2) DEFAULT NULL,
  `sma100s1` decimal(12,2) DEFAULT NULL,
  `sma200s1` decimal(12,2) DEFAULT NULL,
  `adr20` decimal(10,2) DEFAULT NULL,
  `avd20` decimal(18,2) DEFAULT NULL,
  `atr14` decimal(12,2) DEFAULT NULL,
  `a130` decimal(10,2) DEFAULT NULL,
  `a260` decimal(10,2) DEFAULT NULL,
  `a390` decimal(10,2) DEFAULT NULL,
  `ftwh` decimal(12,2) DEFAULT NULL,
  `ftwhdate` date DEFAULT NULL,
  `tswh` decimal(12,2) DEFAULT NULL,
  `tswhdate` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `stockindicators`
--
ALTER TABLE `stockindicators`
  ADD PRIMARY KEY (`symbol`,`date`),
  ADD KEY `ix_si_date` (`date`),
  ADD KEY `ix_si_symbol` (`symbol`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
