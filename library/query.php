<?php namespace Sql\Mysql;

use Sql\Query as SqlQuery;
use Framework\Exception;
use Framework\Extension;

/**
 * Class Query
 * @package Sql\Mysql
 *
 * @property-read Connection $connection
 */
class Query extends SqlQuery {

  /**
   * @param Connection            $connection
   * @param Extension|string|null $prefix
   */
  public function __construct( Connection $connection, $prefix = null ) {
    parent::__construct( $connection, $prefix );
  }

  /**
   * @param array  $commands
   * @param string $raw_command
   * @param array  $insertion
   *
   * @return Result|Result[]
   */
  protected function getResultList( array $commands, $raw_command, array $insertion ) {

    $results = [ ];
    $command = implode( $this->separator, $commands );
    if( !empty( $command ) ) try {

      $link = $this->connection->link;
      @$link->multi_query( $command );

      do {

        // TODO try to detect the proper command for the actual result. The problem is: any procedures may return multiple results

        $tmp          = new \stdClass();
        $tmp->error   = null;
        $tmp->result  = $link->store_result();
        $tmp->command = $command;
        $tmp->rows    = $link->affected_rows;
        $tmp->id      = $link->insert_id;

        if( !$tmp->result ) {

          if( !$link->errno ) $tmp->result = true;
          else $tmp->error = ( new Exception\Strict( self::EXCEPTION_FAIL_QUERY, [
            'command' => $tmp->command,
            'code'    => $link->errno,
            'message' => $link->error
          ] ) )->log();
        }

        $results[ ] = new Result( $tmp->command, $tmp->result, $tmp->error, $tmp->rows, $tmp->id );

      } while( $link->more_results() && $link->next_result() );

    } catch( \Exception $e ) {

      $results[ ] = new Result( $command, false, ( new Exception\Strict( self::EXCEPTION_FAIL_QUERY, [
        'command' => $command,
        'code'    => $e->getCode(),
        'message' => $e->getMessage()
      ], $e ) )->log() );
    }

    // create and return the result object
    $count = count( $results );
    return $count > 1 ? $results : ( $count == 1 ? $results[ 0 ] : null );
  }

  /**
   * Escape input with mysqli method
   *
   * @param string $value The value to escape
   *
   * @return string
   */
  public function escape( $value ) {
    return $this->connection->link->escape_string( (string) $value );
  }
}
