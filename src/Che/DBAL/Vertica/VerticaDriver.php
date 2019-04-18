<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;

/**
 * DBAL Driver for {@link http://www.vertica.com/ Vertica}
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class VerticaDriver implements Driver
{
    /**
     * Ascii semicolon code
     */
    const SEMICOLON = '%3B';

    /**
     * Attempts to create a connection with the database.
     *
     * @param array  $params        All connection parameters passed by the user.
     *                                  - dsn: ODBC dsn, if provided all other parameters are ignored
     *                                  - driver: ODBC Driver name, default to Vertica
     *                                  - host: server host
     *                                  - port: server port
     *                                  - dbname: database name
     * @param string $username      The username to use when connecting.
     * @param string $password      The password to use when connecting.
     * @param array  $driverOptions The driver options to use when connecting.
     *
     * @return Driver\Connection The database connection.
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = [])
    {
        return new ODBCConnection($this->_constructDsn($params), $username, $password);
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform()
    {
        return new VerticaPlatform();
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(Connection $conn)
    {
        return new VerticaSchemaManager($conn);
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'vertica';
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();

        if (isset($params['dbname'])) {
            return $params['dbname'];
        }

        return $conn->query('SELECT CURRENT_DATABASE()')->fetchColumn();
    }

    /**
     * Forms DSN string for connection
     *
     * @param array $params
     *
     * @return string
     */
    private function _constructDsn(array $params)
    {
        if (!empty($params['dsn'])) {
            return $params['dsn'];
        }

        $dsn  = '';
        $dsn .= isset($params['host']) ? 'Servername=' . $params['host'] . ';' : '';
        $dsn .= isset($params['port']) ? 'Port=' . $params['port'] . ';' : '';
        $dsn .= isset($params['dbname']) ? 'Database=' . $params['dbname'] . ';' : '';
        $dsn .= $this->getDriverOptions($params);

        return $dsn;
    }

    /**
     * Forms driver options string
     *
     * @param array $params
     *
     * @return string
     */
    private function getDriverOptions(array $params)
    {
        if (empty($params['driverOptions'])) {
            return '';
        }

        $strOptions = '';
        $options    = $params['driverOptions'];

        $driver = !empty($options['odbc_driver']) ? $options['odbc_driver'] : 'Vertica';
        $strOptions .= "Driver=$driver;";
        $strOptions .= !empty($options['dsn_settings']) ?
            rtrim($options['dsn_settings'], ';') . ';' :
            '';
        $strOptions .= $this->getConnectionSettings($options);

        return $strOptions;
    }

    /**
     * Forms connection settings string
     *
     * @param array $driverOptions
     *
     * @return string
     */
    private function getConnectionSettings(array $driverOptions)
    {
        $settings = [];

        if (!empty($driverOptions['schema'])) {
            $settings[] = "SET search_path='" . $driverOptions['schema'] . "'";
        }

        if (!empty($driverOptions['connection_settings'])) {
            $settings[] = $driverOptions['connection_settings'];
        }

        $settings = str_replace(
            [';', ' '],
            [self::SEMICOLON, '+'],
            implode(self::SEMICOLON, $settings)
        );

        return "ConnSettings=$settings;";
    }
}
