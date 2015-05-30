<?php namespace Sql\Mysql;

use Framework\Exception;
use Sql;

/**
 * Class Transaction
 * @package Sql\Mysql
 */
class Transaction extends Sql\Transaction {

  /**
   * Failed to start a transaction
   */
  const EXCEPTION_FAIL_START = 'sql-mysql#2C';
  /**
   * Failed to commit or rollback a transaction
   */
  const EXCEPTION_FAIL_STOP = 'sql-mysql#3C';

  /**
   * This will start the transaction and set the state flag for the instance
   */
  public function start() {

    $dbr = $this->_query->execute( 'START TRANSACTION' );
    if( $dbr->exception ) throw new Exception\Strict( self::EXCEPTION_FAIL_START, [ ], $dbr->exception );
    else $this->_pending = true;
  }
  /**
   * Stop the transaction with or without rollback action and set the state flag for the instance
   *
   * @param bool $rollback Rollback transaction changes or commit it successfully
   *
   * @throws Exception
   */
  public function stop( $rollback = false ) {

    $dbr = $this->_query->execute( $rollback ? 'ROLLBACK' : 'COMMIT' );
    if( $dbr->exception ) throw new Exception\Strict( self::EXCEPTION_FAIL_STOP, [ ], $dbr->exception );
    else $this->_pending = false;
  }
}
