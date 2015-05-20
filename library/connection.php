<?php namespace Sql\Mysql;

use Sql\Connection as SqlConnection;
use Framework\Exception;
use Framework\Extension;

/**
 * Class Connection
 * @package Sql\Mysql
 *
 * @property-read \mysqli $link     The mysqli connection link
 * @property      string  $database The selected database name
 */
class Connection extends SqlConnection {

  /**
   * Unable to select the database
   */
  const EXCEPTION_FAIL_DATABASE = 'sql-mysql#1C';

  /**
   * The selected database name
   *
   * @var string
   */
  protected $_database = '';

  /**
   * MySQLi connection resource
   *
   * @var \mysqli
   */
  protected $_link;

  /**
   * @param string $index
   *
   * @return mixed
   */
  public function __get( $index ) {
    if( $index == 'link' ) $this->connect();

    $i = '_' . $index;
    if( property_exists( $this, $i ) ) return $this->{$i};
    else return parent::__get( $index );
  }
  /**
   * @param string $index
   * @param mixed  $value
   */
  public function __set( $index, $value ) {
    switch( $index ) {
      case 'database':
        $this->database( $value );
    }
  }
  /**
   * @param string $index
   *
   * @return bool
   */
  public function __isset( $index ) {
    return property_exists( $this, '_' . $index ) || parent::__isset( $index );
  }

  /**
   * Disconnect from the database server
   *
   * @return $this
   */
  public function disconnect() {
    if( $this->_link ) {

      $this->_link->close();
      $this->_link = null;
    }

    return $this;
  }
  /**
   * Create/recreate connection object (link) with database server
   *
   * @param bool $ping Check connection status and reconnect when needed
   *
   * @return $this
   * @throws Exception\System
   */
  protected function connect( $ping = false ) {

    if( !$this->_link || ( $ping && !$this->_link->ping() ) ) {
      $extension = Extension::instance( 'sql-mysql' );

      // create the connection
      $host        = $extension->option( $this->configuration . ':host!string', 'localhost' );
      $this->_link = new \mysqli(
        $host,
        $extension->option( $this->configuration . ':user!string', ini_get( 'mysqli.default_user' ) ),
        $extension->option( $this->configuration . ':password!string', ini_get( 'mysqli.default_pw' ) ),
        '',
        $extension->option( $this->configuration . ':port!number', ini_get( 'mysqli.default_port' ) ),
        $extension->option( $this->configuration . ':socket!string', ini_get( 'mysqli.default_socket' ) )
      );

      // if there was an error, throw one
      if( !$this->_link || $this->_link->connect_errno ) {

        throw ( new Exception\System( self::EXCEPTION_FAIL_CONNECT, [
          'configuration' => $this->_configuration,
          'message'       => $this->_link ? $this->_link->connect_error : 'Unknown'
        ] ) )->log();

      } else {

        // select working database
        $this->database( !empty( $this->_database ) ? $this->_database : $extension->option( $this->configuration . ':database!string' ) );

        // set encoding
        $this->_link->query( 'SET names ' . $extension->option( $this->configuration . ':encoding!string', 'utf8' ) );

        // timezone sync with the PHP
        $date     = new \DateTime( "now", new \DateTimeZone( $extension->option( $this->configuration . ':timezone!string', date_default_timezone_get() ) ) );
        $timezone = sprintf( '%+d:%02d', floor( $date->getOffset() / 60 / 60 ), floor( abs( $date->getOffset() % 60 / 60 ) ) );
        $this->_link->query( "SET time_zone = '{$this->_link->escape_string( $timezone )}'" );
      }
    }

    return $this;
  }

  /**
   * Select a database for the connection
   *
   * @param string $name The database name to select
   *
   * @return $this
   * @throws Exception\System
   */
  protected function database( $name ) {

    if( !$this->_link || !$this->_link->select_db( $name ) ) throw ( new Exception\System( self::EXCEPTION_FAIL_DATABASE, [ $name ] ) )->log();
    else $this->_database = $name;

    return $this;
  }
}
