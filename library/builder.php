<?php namespace Sql\Mysql;

use Sql\Builder as SqlBuilder;

/**
 * Class Builder
 * @package Sql\Mysql
 *
 * TODO add validity checks with error throwing?
 * TODO add support for flags that unset other flags
 */
class Builder extends SqlBuilder {

  /**
   * Build SELECT command based on builder data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/select.html
   *
   * @return string
   */
  public function getSelect() {
    $command = 'SELECT';

    // build starter flags
    $command .= $this->buildFlags( [
      'ALL', 'DISTINCT', 'DISTINCTROW', 'HIGH_PRIORITY', 'MAX_STATEMENT_TIME', 'STRAIGHT_JOIN', 'SQL_SMALL_RESULT',
      'SQL_BIG_RESULT', 'SQL_BUFFER_RESULT', 'SQL_CACHE', 'SQL_NO_CACHE', 'SQL_CALC_FOUND_ROWS'
    ] );

    // adding fields
    $tmp = [ ];
    foreach( $this->fields as $alias => $name ) {
      $tmp[ ] = $this->buildExpression( $name, $alias );
    }
    $command .= count( $tmp ) == 0 ? ' *' : implode( ',', $tmp );

    // adding tables
    $tmp    = [ ];
    $tables = $this->tables;
    foreach( $tables as $alias => $name ) if( !is_array( $name ) ) {
      $tmp[ ] = $this->buildExpression( $name, $alias );
    }
    $command .= ' FROM' . implode( ',', $tmp );

    // adding joins
    foreach( $tables as $alias => $data ) if( is_array( $data ) ) {
      $command .= ' ' . strtoupper( $data[ 'type' ] ) . ' JOIN' . $this->buildExpression( $data[ 'definition' ], $alias ) . ' ON ' . $data[ 'condition' ];
    }

    // add partitions from custom
    if( !empty( $this->customs[ 'partition' ] ) ) {
      $command .= ' PARTITION (' . implode( ',', $this->customs[ 'partition' ] ) . ')';
    }

    // adding where
    $command .= $this->buildFilter( 'where' );

    // adding groups
    $command .= $this->buildList( $this->groups, 'GROUP BY' );
    $command .= $this->buildFlags( [ 'WITH ROLLUP' ] );

    // adding having
    $command .= $this->buildFilter( 'having' );

    // adding orders
    $command .= $this->buildList( $this->orders, 'ORDER BY' );

    // adding limit
    $command .= $this->buildLimit( $this->limit );

    // add procedure from custom
    if( !empty( $this->customs[ 'procedure' ] ) ) {
      $command .= " PROCEDURE {$this->customs['procedure'][0]}(" . implode( ',', array_slice( $this->customs[ 'procedure' ], 1 ) ) . ')';
    }
    $command .= $this->buildFlags( [ 'FOR UPDATE', 'LOCK IN SHARE MODE' ] );

    // adding union custom
    $customs = $this->customs;
    if( isset( $customs[ 'union' ] ) ) {
      foreach( $customs[ 'union' ] as $union_data ) {
        $select = is_array( $union_data ) && count( $union_data ) > 1 ? $union_data[ 1 ] : $union_data;

        if( is_string( $select ) || $select instanceof Builder ) {
          $command .= ' UNION ' . ( is_array( $union_data ) ? ( strtoupper( $union_data[ 0 ] ) . ' ' ) : '' ) . $select;
        }
      }
    }

    // return the builded command
    return $command;
  }
  /**
   * Build INSERT command based on builder data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/insert.html
   *
   * @return string
   */
  public function getInsert() {
    $command = 'INSERT';

    // adding flags
    $flags = [ 'LOW_PRIORITY', 'HIGH_PRIORITY', 'IGNORE' ];
    if( !isset( $this->customs[ 'select' ] ) ) array_splice( $flags, 1, 0, 'DELAYED' );
    $command .= $this->buildFlags( $flags );

    // adding the first table
    foreach( $this->tables as $definition ) if( !is_array( $definition ) ) {
      $command .= $this->buildExpression( $definition, $definition );
      break;
    }

    // add partitions from custom
    if( !empty( $this->customs[ 'partition' ] ) ) {
      $command .= ' PARTITION (' . implode( ',', $this->customs[ 'partition' ] ) . ')';
    }

    // add table fields
    $tmp = [ ];
    foreach( $this->fields as $alias => $definition ) $tmp[ ] = $this->dbq->quoteName( $alias );
    $command .= '(' . implode( ',', $tmp ) . ')';

    // add custom select command if any (and ignore simple and batch insertion)
    if( isset( $this->customs[ 'select' ] ) ) {

      $select = $this->customs[ 'select' ][ 0 ];
      $command .= ' ' . $select;

      // adding fields for batch (mass) insertion
    } else if( $this->exist( 'batch:' ) ) {

      $command .= ' VALUES{field_batch}';
      $this->set( 'field_batch', $this->getArray( 'batch:' ) );

      // adding fields for simple insertion
    } else {

      $value_array = [ ];
      foreach( $this->fields as $alias => $definition ) {
        $value_array[ ] = $alias == $definition ? "{field_{$alias}}" : ( $definition instanceof Builder ? "({$definition})" : $definition );
      }
      $command .= ' VALUES(' . implode( ',', $value_array ) . ')';

      // add fields to the insertion
      $this->each( function ( $key, $value, $index, $builder ) {
        /** @var Builder $builder */
        $builder->set( 'field_' . $key, $value );
      }, 'field:' );
    }

    // add duplicate conditions
    if( isset( $this->customs[ 'on duplicate' ] ) ) {
      $command .= ' ON DUPLICATE KEY UPDATE ' . implode( ',', $this->customs[ 'on duplicate' ] );
    }

    // return the builded command
    return $command;
  }
  /**
   * Build UPDATE command based on builder data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/update.html
   *
   * @return string
   */
  public function getUpdate() {
    $command = 'UPDATE';

    // adding flags
    $command .= $this->buildFlags( [ 'LOW_PRIORITY', 'IGNORE' ] );

    // adding tables
    $tmp = [ ];
    foreach( $this->tables as $alias => $definition ) if( !is_array( $definition ) ) {
      $tmp[ ] = $this->buildExpression( $definition, $alias );
    }
    $command .= implode( ', ', $tmp );

    // adding joins
    foreach( $this->tables as $alias => $data ) if( is_array( $data ) ) {
      $command .= ' ' . strtoupper( $data[ 'type' ] ) . ' JOIN' . $this->buildExpression( $data[ 'definition' ], $alias ) . ' ON ' . $data[ 'condition' ];
    }

    // adding fields
    $fields      = $this->fields;
    $field_array = [ ];
    foreach( $fields as $alias => $definition ) {
      $field_array[ ] = $alias . ' = ' . ( $alias == $definition ? "{field_{$alias}}" : ( $definition instanceof Builder ? "({$definition})" : $definition ) );
    }
    $command .= ' SET ' . implode( ', ', $field_array );

    // add fields to the insertion
    $this->each( function ( $key, $value, $index, $builder ) {
      /** @var Builder $builder */
      $builder->set( 'field_' . $key, $value );
    }, 'field:' );

    // adding filters
    $command .= $this->buildFilter( 'where' );

    // add single table plus
    if( count( $this->tables ) == 1 ) {

      // adding orders
      $command .= $this->buildList( $this->orders, 'ORDER BY' );

      // adding limit
      $command .= $this->buildLimit( $this->limit );
    }

    // return the builded command
    return $command;
  }
  /**
   * Build DELETE command based on builder data
   *
   * http://dev.mysql.com/doc/refman/5.7/en/delete.html
   *
   * @return string
   */
  public function getDelete() {
    $command = 'DELETE';

    // decide to use multi table or single table style
    $multi = count( $this->tables ) > 1;

    // adding flags
    $command .= $this->buildFlags( [ 'LOW_PRIORITY', 'QUICK', 'IGNORE' ] );

    // adding fields that is tables in this context of Builder
    if( $multi ) {

      foreach( $this->fields as $alias => $name ) $field_array[ ] = $this->buildExpression( $name, $alias );
      $command .= empty( $field_array ) ? '' : implode( ',', $field_array );
    }

    // preparing tables
    $tmp = [ ];
    foreach( $this->tables as $alias => $name ) if( !is_array( $name ) ) {

      // alias not available in single table
      $tmp[ ] = $this->buildExpression( $name, $multi ? $alias : $name );
    }

    // add tables
    $command .= ' FROM ' . implode( ',', $tmp );

    // adding joins in multitable syntax
    if( $multi ) {

      foreach( $this->tables as $alias => $data ) if( is_array( $data ) ) {
        $command .= ' ' . strtoupper( $data[ 'type' ] ) . ' JOIN ' . $this->buildExpression( $data[ 'definition' ], $alias ) . ' ON ' . $data[ 'condition' ];
      }
    }

    // add partitions in single table syntax
    if( !$multi && !empty( $this->customs[ 'partition' ] ) ) {
      $command .= ' PARTITION (' . implode( ',', $this->customs[ 'partition' ] ) . ')';
    }

    // adding filters
    $command .= $this->buildFilter( 'where' );

    if( !$multi ) {

      // adding orders
      $command .= $this->buildList( $this->orders, 'ORDER BY' );

      // adding limit
      $command .= $this->buildLimit( $this->limit );
    }

    // return the builded command
    return $command;
  }

  /**
   * Build stored flags based on the input
   *
   * @since 0.10.0
   *
   * @param array $include Only include this flags (if any) and in this order
   *
   * @return string
   */
  protected function buildFlags( array $include ) {

    $result = '';
    $flags  = array_flip( array_map( 'strtoupper', $this->flags ) );
    foreach( $include as $flag ) if( array_key_exists( $flag, $flags ) ) {
      $result .= ' ' . $flag;
    }

    return $result;
  }
  /**
   * Build expression with alias. This will convert Builder class to valid expression
   *
   * @since 0.10.0
   *
   * @param string|Builder $expression
   * @param string         $alias
   *
   * @return string
   */
  protected function buildExpression( $expression, $alias = null ) {

    $expression = $expression instanceof Builder ? "({$expression})" : $expression;
    $alias      = empty( $alias ) || $alias == $expression ? '' : ( ' AS ' . $this->dbq->quoteName( $alias ) );

    return ' ' . $expression . $alias;
  }
  /**
   * Build a filter string with the stored data
   *
   * @since 0.10.0
   *
   * @param string $type Filter type to build
   *
   * @return string
   */
  protected function buildFilter( $type ) {

    $imploded = '';
    if( !empty( $this->filters[ $type ] ) ) {

      foreach( $this->filters[ $type ] as $data ) {
        $imploded .= ( empty( $imploded ) ? '' : ( ' ' . strtoupper( $data[ 'glue' ] ) . ' ' ) ) . $this->buildExpression( $data[ 'expression' ] );
      }

      $imploded = ' ' . strtoupper( $type ) . ' ' . $imploded;
    }

    return $imploded;
  }
  /**
   * Build order and group like lists
   *
   * @since 0.10.0
   *
   * @param array  $input
   * @param string $type
   *
   * @return array|string
   */
  protected function buildList( array $input, $type ) {

    $result = [ ];
    foreach( $input as $data ) {
      $result[ ] = $this->buildExpression( $data[ 0 ] ) . ( isset( $data[ 1 ] ) ? ( ' ' . strtoupper( $data[ 1 ] ) ) : '' );
    }

    if( !count( $input ) ) $result = '';
    else $result = " {$type}" . implode( ',', $result );

    return $result;
  }
  /**
   * Build limit string
   *
   * @since 0.10.0
   *
   * @param array $input
   *
   * @return string
   */
  protected function buildLimit( array $input ) {

    foreach( $input as &$tmp ) if( $tmp != 0 ) {
      $tmp = trim( $this->buildExpression( $tmp ) );
    }

    return count( $input ) > 1 && $input[ 1 ] > 0 ? ( ' LIMIT ' . ( $input[ 0 ] > 0 ? implode( ', ', $input ) : $input[ 1 ] ) ) : '';
  }
}
