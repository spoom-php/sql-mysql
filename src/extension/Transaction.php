<?php namespace Spoom\Sql\MySQL;

use Spoom\Core\Exception;
use Spoom\Sql;
use Spoom\Core\Helper;

/**
 * @since 1.1.0
 *
 * @property-read bool       $pending
 * @property-read Connection $connection
 */
class Transaction extends Sql\Transaction implements Helper\AccessableInterface {
  use Helper\Accessable;

  /**
   * @var bool
   */
  private $_pending;
  /**
   * @var Connection
   */
  private $_connection;

  /**
   * @param Connection $connection
   */
  public function __construct( Connection $connection ) {
    $this->_connection = $connection;
    $this->_pending    = false;
  }

  //
  public function begin( ?string $savepoint = null ) {
    if( !$this->_pending ) {

      $resource = $this->_connection->getResource( true );
      if( @!$resource->begin_transaction( 0, $savepoint ) ) throw new Sql\TransactionException( 'begin', $this->_connection, Connection::exception( $resource ) );
      else $this->_pending = true;
    }
  }
  //
  public function savepoint( string $name ) {
    if( $this->_pending ) {

      $resource = $this->_connection->getResource();
      if( @!$resource->savepoint( $name ) ) {
        throw new Sql\TransactionException( 'savepoint', $this->_connection, Connection::exception( $resource ) );
      }
    }
  }
  //
  public function rollback( ?string $savepoint = null ) {
    if( $this->_pending ) {

      $resource = $this->_connection->getResource();
      /** @noinspection PhpMethodParametersCountMismatchInspection */
      if( @!$resource->rollback( 0, $savepoint ) ) {

        Exception::log( new Sql\TransactionException( 'rollback', $this->_connection, Connection::exception( $resource ) ) );

        // FIXME i'm not sure...what will happen to the trasnaction if it can't be rolled back?!
        $this->_pending = false;
      } else if( !$savepoint ) $this->_pending = false;
    }
  }
  //
  public function commit() {
    if( $this->_pending ) {

      $resource = $this->_connection->getResource();
      if( @!$resource->commit() ) {

        Exception::log( new Sql\TransactionException( 'commit', $this->_connection, Connection::exception( $resource ) ) );

        // FIXME i'm not sure...what will happen to the trasnaction if it can't be commited?!
      }

      $this->_pending = false;
    }
  }

  //
  public function isPending(): bool {
    return $this->_pending;
  }
  //
  public function getConnection(): Sql\ConnectionInterface {
    return $this->_connection;
  }
}
