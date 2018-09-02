<?php namespace Spoom\Sql\MySQL;

use Spoom\Core\Exception;
use Spoom\Sql;
use Spoom\Sql\TransactionInterface;

/**
 * @property-read \mysqli $resource
 */
class Connection extends Sql\Connection {

  /**
   * @var static[]
   */
  protected static $INSTANCE = [];

  /**
   * Host and port definition
   *
   * @var string
   */
  private $_uri;
  /**
   * @var null|string
   */
  private $user;
  /**
   * @var null|string
   */
  private $password;
  /**
   * @var array
   */
  private $_option = [];
  /**
   * The selected database name
   *
   * @var string
   */
  private $_database = '';
  /**
   * @var TransactionInterface
   */
  private $_transaction;

  /**
   * MySQLi connection resource
   *
   * @var \mysqli
   */
  protected $_resource;

  /**
   * @param string      $uri
   * @param string      $user
   * @param null|string $password
   * @param string      $database
   * @param array       $option
   */
  public function __construct( string $uri, string $user, ?string $password, string $database = '', array $option = [] ) {

    $this->_uri = strpos( $uri, ':' ) === false ? ( $uri . ':3306' ) : $uri;
    $this->setAuthentication( $user, $password );
    $this->setDatabase( $database );

    $this->setOption( $option );
  }

  //
  function __clone() {

    // reset internal connection state
    $this->_resource    = null;
    $this->_transaction = null;
  }

  //
  public function statement(): Sql\Expression\StatementInterface {
    return new Expression\Statement( $this );
  }

  //
  public function execute( $statement, $context = [] ) {

    // pre-process the statement(s)
    $statements = is_array( $statement ) ? $statement : [ $statement ];

    $query = '';
    foreach( $statements as $i => $tmp ) {
      $statements[ $i ] = $this->apply( $tmp, $context ) . static::CHARACTER_SEPARATOR;
      $query            .= $statements[ $i ];
    }

    $result_list = [];
    if( !empty( $query ) ) try {

      $result = @$this->getResource( true )->multi_query( $query );
      do {

        // TODO try to detect the proper command for the actual result. The problem is: any procedures may return multiple results

        $tmp           = $result ? $this->_resource->store_result() : null;
        $result_list[] = new Result(
          $query,
          $tmp,
          !$tmp && $this->_resource->errno ? new Sql\Expression\StatementException( $query, $this, static::exception( $this->_resource ) ) : null,
          $this->_resource->affected_rows,
          $this->_resource->insert_id
        );

      } while( $result && $this->_resource->more_results() && @$this->_resource->next_result() );

    } catch( \mysqli_sql_exception $e ) {
      $result_list = [ new Result( $query, null, new Sql\Expression\StatementException( $query, $this, $e ) ) ];
    }

    // create and return the result object
    return is_array( $statement ) ? $result_list : ( !empty( $statement ) ? $result_list[ 0 ] : null );
  }
  //
  public function escape( string $text ): string {
    return $this->getResource()->escape_string( (string) $text );
  }

  //
  public function connect( bool $ping = false ) {

    if( !$this->_resource || ( $ping && !$this->_resource->ping() ) ) {
      $this->_resource = null;

      // create the connection
      list( $host, $port ) = explode( ':', $this->getUri() );
      $password = null;
      $user     = $this->getAuthentication( $password );

      if( !function_exists( 'mysqli_init' ) || !( $resource = mysqli_init() ) ) throw new \RuntimeException( 'Missing PHP extension: mysqli' );
      else try {

        // TODO apply options to the resource
        // TODO build flags from the options

        // connect (for real..)
        // TODO add support for socket
        if( @!mysqli_real_connect( $resource, $host, $user, $password, $this->getDatabase(), $port ) ) {
          throw static::exception( $resource );
        }

        // TODO detect and validate server version

        // apply 'after' connection options
        $option = [ 'encoding', 'timezone' ];
        foreach( $option as $name ) try {
          $this->option( $name, $resource );
        } catch( Sql\ConnectionOptionException $e ) {
          Exception::log( $e );
        }

        // 
        $this->_resource = $resource;

      } catch( \mysqli_sql_exception $e ) {
        throw new Sql\ConnectionException( $this, $e );
      }
    }

    return $this;
  }
  //
  public function disconnect() {
    if( $this->_resource ) {

      @$this->_resource->close();
      $this->_resource = null;
    }

    return $this;
  }

  /**
   * Apply option to a connection resource
   *
   * @param string  $name
   * @param \mysqli $resource
   *
   * @throws Sql\ConnectionOptionException
   */
  protected function option( $name, \mysqli $resource ) {

    $option = $this->getOption( $name );
    try {

      switch( $name ) {

        // change the default connection encoding
        case 'encoding':
          if( $option && @!$resource->set_charset( $option ) ) {
            throw static::exception( $resource );
          }

          break;

        // change the connection's default timezone
        case 'timezone':

          if( $option ) {

            $date     = new \DateTime( 'now', new \DateTimeZone( $option ) );
            $timezone = $resource->real_escape_string( sprintf( '%+d:%02d', floor( $date->getOffset() / 60 / 60 ), floor( abs( $date->getOffset() % 60 / 60 ) ) ) );

            if( @!$resource->query( "SET time_zone = '{$timezone}'" ) ) {
              throw static::exception( $resource );
            }
          }
      }

    } catch( \mysqli_sql_exception $e ) {
      throw new Sql\ConnectionOptionException( $name, $option, $this, $e );
    }
  }

  //
  public function isConnected( bool $ping = true ): bool {
    return isset( $this->_resource ) && ( !$ping || $this->_resource->ping() );
  }
  //
  public function getUri(): string {
    return $this->_uri;
  }
  //
  public function getAuthentication( &$password = null ): ?string {
    $password = $this->password;
    return $this->user;
  }
  //
  public function setAuthentication( ?string $user, ?string $password = null ) {
    if( $this->isConnected() && @!$this->_resource->change_user( $user, $password, null ) ) {
      throw new Sql\ConnectionOptionException( 'authentication', $user . ':' . $password, $this, static::exception( $this->_resource ) );
    }

    $this->user     = $user;
    $this->password = $password;
  }
  //
  public function getOption( ?string $name = null, $default = null ) {
    return isset( $name ) ? ( $this->_option[ $name ]??$default ) : $this->_option;
  }
  //
  public function setOption( $value, ?string $name = null ) {

    $value = $name ? [ $name => $value ] : $value;
    foreach( $value as $name => $option ) {

      // apply option to the connection if it's alive
      if( $this->isConnected( false ) ) {
        $this->option( $name, $option );
      }

      // save option for later
      $this->_option[ $name ] = $option;
    }

    return $this;
  }
  //
  public function getDatabase(): ?string {
    return $this->_database;
  }
  //
  public function setDatabase( ?string $value ) {
    if( $this->isConnected() && @!$this->getResource( true )->select_db( $value ) ) {
      throw new Sql\ConnectionOptionException( 'database', $value, $this, static::exception( $this->_resource ) );
    }

    $this->_database = $value;
  }
  //
  public function getTransaction(): TransactionInterface {
    return $this->_transaction ?? ( $this->_transaction = new Transaction( $this ) );
  }

  /**
   * @param bool $ping {@see static::connect()}
   *
   * @return \mysqli
   */
  public function getResource( bool $ping = false ): \mysqli {

    $this->connect( $ping );
    return $this->_resource;
  }

  /**
   * Create exception from a connection resource
   *
   * @param \mysqli $resource
   *
   * @return \mysqli_sql_exception|null
   */
  public static function exception( \mysqli $resource ) {
    return $resource->errno ? new \mysqli_sql_exception( $resource->error, $resource->errno ) : null;
  }
  /**
   * @param string $configuration
   * @param bool   $force
   *
   * @return static
   * @throws \RuntimeException
   */
  public static function instance( string $configuration = 'default', $force = false ) {

    if( $force || !isset( static::$INSTANCE[ $configuration ] ) ) {

      $mysql = Extension::instance()->getConfiguration();
      if( !isset( $mysql[ 'connection:' . $configuration ] ) ) throw new \RuntimeException( "Missing MySQL configuration: {$configuration}" );
      else static::$INSTANCE[ $configuration ] = new static(
        $mysql[ "connection:{$configuration}.host" ],
        $mysql[ "connection:{$configuration}.user" ],
        $mysql[ "connection:{$configuration}.password" ],
        $mysql[ "connection:{$configuration}.database" ],
        $mysql[ "connection:{$configuration}.option!array" ]
      );
    }

    return static::$INSTANCE[ $configuration ];
  }
}
