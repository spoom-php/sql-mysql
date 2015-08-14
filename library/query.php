<?php namespace Sql\Mysql;

use Framework\Exception;
use Framework\Extension;
use Sql;

/**
 * Class Query
 * @package Sql\Mysql
 *
 * @property-read Connection $connection
 */
class Query extends Sql\Query {

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

    $result_list = [ ];
    $command     = implode( $this->separator, $commands );
    if( !empty( $command ) ) try {

      $link   = $this->connection->link;
      $result = @$link->multi_query( $command );

      do {

        // TODO try to detect the proper command for the actual result. The problem is: any procedures may return multiple results

        $tmp = (object) [
          'error'   => null,
          'result'  => $result ? $link->store_result() : false,
          'command' => $command,
          'rows'    => $link->affected_rows,
          'id'      => $link->insert_id
        ];

        if( !$tmp->result ) {

          if( !$link->errno ) $tmp->result = true;
          else $tmp->error = new Exception\Strict( self::EXCEPTION_FAIL_QUERY, [
            'command' => $tmp->command,
            'code'    => $link->errno,
            'message' => $link->error
          ] );
        }

        $result_list[] = new Result( $tmp->command, $tmp->result, $tmp->error, $tmp->rows, $tmp->id );

      } while( $result && $link->more_results() && @$link->next_result() );

    } catch( \Exception $e ) {

      $result_list[] = new Result( $command, false, new Exception\Strict( self::EXCEPTION_FAIL_QUERY, [
        'command' => $command,
        'code'    => $e->getCode(),
        'message' => $e->getMessage()
      ], $e ) );
    }

    // create and return the result object
    $count = count( $result_list );
    return $count > 1 ? $result_list : ( $count == 1 ? $result_list[ 0 ] : null );
  }

  /**
   * Escape input with mysqli method
   *
   * @param string $value The value to escape
   *
   * @return string
   */
  public function escape( $value ) {
    return @$this->connection->link->escape_string( (string) $value );
  }
}
