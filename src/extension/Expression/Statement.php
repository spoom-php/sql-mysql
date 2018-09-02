<?php namespace Spoom\Sql\MySQL\Expression;

use Spoom\Sql;
use Spoom\Sql\ConnectionInterface;

/**
 * TODO add validity checks with error throwing?
 * TODO add support for flags that unset other flags
 */
class Statement extends Sql\Expression\Statement {

  const CUSTOM_PARTITION   = 'partition';
  const CUSTOM_SELECT      = 'select';
  const CUSTOM_PROCEDURE   = 'procedure';
  const CUSTOM_UNION       = 'union';
  const CUSTOM_ONDUPLICATE = 'on duplicate';

  const FLAG_LOWPRIORITY      = 'LOW_PRIORITY';
  const FLAG_HIGHPRIORITY     = 'HIGH_PRIORITY';
  const FLAG_IGNORE           = 'IGNORE';
  const FLAG_DELAYED          = 'DELAYED';
  const FLAG_ALL              = 'ALL';
  const FLAG_DISTINCT         = 'DISTINCT';
  const FLAG_DISTINCTROW      = 'DISTINCTROW';
  const FLAG_MAXSTATEMENTTIME = 'MAX_STATEMENT_TIME';
  const FLAG_STRAIGHTJOIN     = 'STRAIGHT_JOIN';
  const FLAG_SMALLRESULT      = 'SQL_SMALL_RESULT';
  const FLAG_BIGRESULT        = 'SQL_BIG_RESULT';
  const FLAG_BUFFERRESULT     = 'SQL_BUFFER_RESULT';
  const FLAG_CACHE            = 'SQL_CACHE';
  const FLAG_NOCACHE          = 'SQL_NO_CACHE';
  const FLAG_CALCFOUNDROWS    = 'SQL_CALC_FOUND_ROWS';
  const FLAG_FORUPDATE        = 'FOR UPDATE';
  const FLAG_LOCKINSHAREMODE  = 'LOCK IN SHARE MODE';
  const FLAG_QUICK            = 'QUICK';
  const FLAG_WITHROLLUP       = 'WITH ROLLUP';

  public function __construct( ConnectionInterface $connection, $context = [] ) {
    parent::__construct( $connection, $context );

    // enable MySQL custom features
    $this->supportCustom( [
      static::CUSTOM_PARTITION,
      static::CUSTOM_ONDUPLICATE,
      static::CUSTOM_PROCEDURE,
      static::CUSTOM_SELECT,
      static::CUSTOM_UNION
    ] );
  }

  /**
   * Build SELECT command based on statement data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/select.html
   *
   * @return string
   */
  public function getSelect() {

    $context = $this->getContext();
    $command = 'SELECT';

    // build starter flags
    $command .= $this->renderFlag( [
      static::FLAG_ALL, static::FLAG_DISTINCT, static::FLAG_DISTINCTROW, static::FLAG_HIGHPRIORITY,
      static::FLAG_MAXSTATEMENTTIME, static::FLAG_STRAIGHTJOIN, static::FLAG_SMALLRESULT, static::FLAG_BIGRESULT,
      static::FLAG_BUFFERRESULT, static::FLAG_CACHE, static::FLAG_NOCACHE, static::FLAG_CALCFOUNDROWS
    ] );

    // adding fields
    $tmp = [];
    foreach( $this->getField() as $alias => $field ) {

      $index = static::CONTEXT_FIELD . '.0.' . $alias;
      $tmp[] = $this->renderAlias( isset( $context[ $index ] ) ? "{{$index}}" : $field, $alias );
    }
    $command .= count( $tmp ) == 0 ? ' *' : implode( ',', $tmp );

    // adding tables (with joins)
    if( !empty( $tmp = $this->renderTableList() ) ) {
      $command .= ' FROM' . $tmp;
      $command .= $this->renderTableJoinList();
    }

    // add partitions from custom
    if( ( $tmp = $this->getCustom( static::CUSTOM_PARTITION ) ) ) {
      $command .= ' PARTITION (' . implode( ',', $tmp ) . ')';
    }

    // adding where
    $command .= $this->renderFilterList( static::FILTER_SIMPLE );

    // adding groups
    $command .= $this->renderList( $this->getGroup(), 'GROUP BY' );
    $command .= $this->renderFlag( [ static::FLAG_WITHROLLUP ] );

    // adding having
    $command .= $this->renderFilterList( static::FILTER_GROUP );

    // adding orders, limit
    $command .= $this->renderList( $this->getSort(), 'ORDER BY' );
    $command .= $this->renderLimit();

    // add procedure from custom
    if( ( $tmp = $this->getCustom( static::CUSTOM_PROCEDURE ) ) ) {
      $command .= " PROCEDURE {$tmp[0]}(" . implode( ',', array_slice( $tmp, 1 ) ) . ')';
    }
    $command .= $this->renderFlag( [ static::FLAG_FORUPDATE, static::FLAG_LOCKINSHAREMODE ] );

    // adding union custom
    if( ( $tmp = $this->getCustom( static::CUSTOM_UNION ) ) ) {
      foreach( $tmp as $union_data ) {

        $select  = is_array( $union_data ) && count( $union_data ) > 1 ? $union_data[ 1 ] : $union_data;
        $command .= ' UNION ' . ( is_array( $union_data ) ? ( strtoupper( $union_data[ 0 ] ) . ' ' ) : '' ) . $select;
      }
    }

    // return the builded command
    return $command;
  }
  /**
   * Build INSERT command based on statement data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/insert.html
   *
   * @return string
   */
  public function getInsert() {

    $context = $this->getContext();
    $command = 'INSERT';

    // adding flags
    $flags = [ static::FLAG_LOWPRIORITY, static::FLAG_HIGHPRIORITY, static::FLAG_IGNORE ];
    if( !$this->getCustom( static::CUSTOM_SELECT ) ) array_splice( $flags, 1, 0, static::FLAG_DELAYED );
    $command .= $this->renderFlag( $flags );

    // adding the first table
    foreach( $this->getTable() as $table ) if( empty( $table[ 'filter' ] ) ) {
      $command .= ' ' . $table[ 'definition' ];
      break;
    }

    // add partitions from custom
    if( ( $tmp = $this->getCustom( static::CUSTOM_PARTITION ) ) ) {
      $command .= ' PARTITION (' . implode( ',', $tmp ) . ')';
    }

    // add table fields
    $fields = array_keys( $this->getField() );
    if( !empty( $fields ) ) $command .= $this->getConnection()->quoteName( $fields );

    // add custom select command if any (and ignore simple and batch insertion)
    if( ( $tmp = $this->getCustom( static::CUSTOM_SELECT ) ) ) $command .= ' ' . $tmp[ 0 ];
    else {

      // process the fields to match with the headers
      $batch = [];
      foreach( $context[ static::CONTEXT_FIELD ] as $slot => $field_list ) {

        $list = [];
        foreach( $fields as $i => $alias ) {

          $field = $this->getField( $alias );
          if( $field != $alias ) $list[] = is_object( $field ) && $field instanceof Sql\Expression ? $field : $this->getConnection()->expression( $field );
          else if( array_key_exists( $alias, $field_list ) ) $list[] = $field_list[ $alias ];
          else if( array_key_exists( $i, $field_list ) ) $list[] = $field_list[ $i ];
          else $list[] = $this->getConnection()->name( 'DEFAULT' );
        }

        $batch[] = $list;
      }

      $context[ '_' . static::CONTEXT_FIELD ] = $batch;
      $command                                .= ' VALUES{_' . static::CONTEXT_FIELD . '}';
    }

    // add duplicate conditions
    if( ( $tmp = $this->getCustom( static::CUSTOM_ONDUPLICATE ) ) ) {
      // TODO support simple definitions in [ 'field', 'value' ] form or something
      $command .= ' ON DUPLICATE KEY UPDATE ' . implode( ',', $tmp );
    }

    // return the builded command
    return $command;
  }
  /**
   * Build UPDATE command based on statement data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/update.html
   *
   * @return string
   */
  public function getUpdate() {

    $context    = $this->getContext();
    $field_list = $context[ static::CONTEXT_FIELD . '.0' ];
    $command    = 'UPDATE';

    // adding flags
    $command .= $this->renderFlag( [ static::FLAG_LOWPRIORITY, static::FLAG_IGNORE ] );

    // adding tables (with joins)
    $command .= $this->renderTableList();
    $command .= $this->renderTableJoinList();

    // adding fields
    $field_array = [];
    foreach( $this->getField() as $alias => $field ) {

      $field = $this->getField( $alias );
      if( $field != $alias ) $_field = is_object( $field ) && $field instanceof Sql\Expression ? $field : $this->getConnection()->expression( $field );
      else if( array_key_exists( $alias, $field_list ) ) $_field = $field_list[ $alias ];
      else $_field = $this->getConnection()->name( 'DEFAULT' );

      $field_array[] = $this->renderOperator( $this->getConnection()->apply( '{0}', [ $_field ] ), $alias, '=' );
    }
    $command .= ' SET' . implode( ',', $field_array );

    // adding filters
    $command .= $this->renderFilterList( static::FILTER_SIMPLE );

    // add single table plus
    if( count( $this->getTable() ) == 1 ) {
      $command .= $this->renderList( $this->getSort(), 'ORDER BY' );
      $command .= $this->renderLimit();
    }

    return $command;
  }
  /**
   * Build DELETE command based on statement data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/delete.html
   *
   * @return string
   */
  public function getDelete() {
    $command = 'DELETE';

    // decide to use multi table or single table style
    $multi = count( $this->getTable() ) > 1;

    // adding flags
    $command .= $this->renderFlag( [ static::FLAG_LOWPRIORITY, static::FLAG_QUICK, static::FLAG_IGNORE ] );

    // adding fields that is tables in multi delete syntax
    if( $multi && ( $tmp = array_keys( $this->getField() ) ) ) {
      $command .= ' ' . $this->getConnection()->quoteName( $tmp, false );
    }

    // adding tables (and joins in multitable syntax)
    $command .= ' FROM' . $this->renderTableList();
    if( $multi ) $command .= $this->renderTableJoinList();

    // add partitions in single table syntax
    if( !$multi && !( $tmp = $this->getCustom( static::CUSTOM_PARTITION ) ) ) {
      $command .= ' PARTITION (' . implode( ',', $tmp ) . ')';
    }

    //
    $command .= $this->renderFilterList( static::FILTER_SIMPLE );

    // adding orders and limit only for single table syntax
    if( !$multi ) {
      $command .= $this->renderList( $this->getSort(), 'ORDER BY' );
      $command .= $this->renderLimit();
    }

    return $command;
  }

  /**
   * Create table(s) that has no condition
   *
   * @return string
   */
  protected function renderTableList() {

    $tmp = [];
    foreach( $this->getTable() as $alias => $table ) if( empty( $table[ 'filter' ] ) ) {
      $tmp[] = $this->renderAlias( $table[ 'definition' ], $alias );
    }

    return implode( ',', $tmp );
  }
  /**
   * Create table(s) that has condition
   *
   * @return string
   */
  protected function renderTableJoinList() {

    $command = '';
    foreach( $this->getTable() as $alias => $table ) if( !empty( $table[ 'filter' ] ) ) {
      $command .= ' ' . strtoupper( $table[ 'type' ] ) . ' JOIN' . $this->renderAlias( $table[ 'definition' ], $alias ) . ' ON ' . $table[ 'filter' ];
    }

    return $command;
  }
  /**
   * Create stored flags based on the input
   *
   * @param array $include Only include this flags (if any) and in this order
   *
   * @return string
   */
  protected function renderFlag( array $include ) {

    $result = '';
    $flags  = array_flip( array_map( 'strtoupper', $this->getFlag() ) );
    foreach( $include as $flag ) if( array_key_exists( $flag, $flags ) ) {
      $result .= ' ' . $flag;
    }

    return $result;
  }
  /**
   * Create a filter string with the stored data
   *
   * @param string $type Filter type to build
   *
   * @return string
   */
  protected function renderFilterList( $type ) {
    $context = $this->getContext();

    $imploded = '';
    if( !empty( $tmp = $this->getFilter( $type ) ) ) {

      $imploded = '(' . implode( ') AND (', $tmp ) . ')';
      foreach( $context[ static::CONTEXT_FILTER . '.' . $type ] as $name => $_ ) {
        $imploded = preg_replace( "/\\{([\W]*){$name}(\\.\\})?/", '{$1' . static::CONTEXT_FILTER . '.' . $type . '.' . $name . '$2', $imploded );
      }

      $imploded = ' ' . strtoupper( $type ) . ' ' . $imploded;
    }

    return $imploded;
  }
  /**
   * Create order and group like lists
   *
   * @param array  $input
   * @param string $type
   *
   * @return array|string
   */
  protected function renderList( array $input, $type ) {

    $result = [];
    foreach( $input as $data ) {
      $result[] = ' ' . $data[ 0 ] . ( isset( $data[ 1 ] ) ? ( ' ' . strtoupper( $data[ 1 ] ) ) : '' );
    }

    if( !count( $input ) ) $result = '';
    else $result = " {$type}" . implode( ',', $result );

    return $result;
  }
  /**
   * Create limit string
   *
   * @return string
   */
  protected function renderLimit() {

    $limit = $this->getLimit( $offset );
    return $limit > 0 ? ( ' LIMIT ' . ( $offset > 0 ? implode( ', ', [ $offset, $limit ] ) : $limit ) ) : '';
  }

  /**
   * Create expression with(out) alias
   *
   * @param string|Statement $expression
   * @param string           $alias
   *
   * @return string
   */
  protected function renderAlias( $expression, $alias ) {
    return ' ' . $expression . ( empty( $alias ) || $alias == $expression ? '' : ( ' AS ' . $this->getConnection()->quoteName( $alias ) ) );
  }
  /**
   * Create " {field} {operator} {expression}" like strings
   *
   * @param string|Statement $expression
   * @param string|null      $field
   * @param string|null      $operator
   *
   * @return string
   */
  protected function renderOperator( $expression, $field, $operator ) {
    return ' ' . $this->getConnection()->quoteName( $field ) . ' ' . $operator . ' ' . $expression;
  }
}
