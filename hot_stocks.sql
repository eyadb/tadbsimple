-- phpMyAdmin SQL Dump
-- version 5.2.2
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Oct 30, 2025 at 12:35 AM
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
-- Table structure for table `hot_stocks`
--

CREATE TABLE `hot_stocks` (
  `id` int(11) NOT NULL,
  `symbol` varchar(20) NOT NULL,
  `date` date NOT NULL,
  `open` decimal(10,2) DEFAULT NULL,
  `close` decimal(10,2) DEFAULT NULL,
  `price_change_pct` decimal(10,2) DEFAULT NULL,
  `volume` bigint(20) DEFAULT NULL,
  `volume_ratio` decimal(10,2) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `hot_stocks`
--
ALTER TABLE `hot_stocks`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `unique_symbol_date` (`symbol`,`date`),
  ADD KEY `idx_date` (`date`),
  ADD KEY `idx_price_change` (`price_change_pct`),
  ADD KEY `idx_volume_ratio` (`volume_ratio`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `hot_stocks`
--
ALTER TABLE `hot_stocks`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
