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
   * Pattern for MySQL identifier names
   *
   * @since 1.1.2
   */
  const PATTERN_IDENTIFIER = '/^[a-z0-9\\$\\_]+$/i';

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
      static::FLAG_ALL, static::FLAG_DISTINCT, static::FLAG_DISTINCTROW, static::FLAG_HIGHPRIORITY,
      static::FLAG_MAXSTATEMENTTIME, static::FLAG_STRAIGHTJOIN, static::FLAG_SMALLRESULT, static::FLAG_BIGRESULT,
      static::FLAG_BUFFERRESULT, static::FLAG_CACHE, static::FLAG_NOCACHE, static::FLAG_CALCFOUNDROWS
    ] );

    // adding fields
    $tmp = [ ];
    foreach( $this->fields as $alias => $name ) {
      $tmp[] = $this->buildExpression( $name, $alias );
    }
    $command .= count( $tmp ) == 0 ? ' *' : implode( ',', $tmp );

    // adding tables
    $tmp    = [ ];
    $tables = $this->tables;
    foreach( $tables as $alias => $name ) if( !is_array( $name ) ) {
      $tmp[] = $this->buildExpression( $name, $alias );
    }
    $command .= ' FROM' . implode( ',', $tmp );

    // adding joins
    foreach( $tables as $alias => $data ) if( is_array( $data ) ) {
      $command .= ' ' . strtoupper( $data[ 'type' ] ) . ' JOIN' . $this->buildExpression( $data[ 'definition' ], $alias ) . ' ON ' . $data[ 'condition' ];
    }

    // add partitions from custom
    if( !empty( $this->customs[ static::CUSTOM_PARTITION ] ) ) {
      $command .= ' PARTITION (' . implode( ',', $this->customs[ static::CUSTOM_PARTITION ] ) . ')';
    }

    // adding where
    $command .= $this->buildFilter( 'where' );

    // adding groups
    $command .= $this->buildList( $this->groups, 'GROUP BY' );
    $command .= $this->buildFlags( [ static::FLAG_WITHROLLUP ] );

    // adding having
    $command .= $this->buildFilter( 'having' );

    // adding orders
    $command .= $this->buildList( $this->orders, 'ORDER BY' );

    // adding limit
    $command .= $this->buildLimit( $this->limit );

    // add procedure from custom
    if( !empty( $this->customs[ static::CUSTOM_PROCEDURE ] ) ) {
      $tmp = $this->customs[ static::CUSTOM_PROCEDURE ];
      $command .= " PROCEDURE {$tmp[0]}(" . implode( ',', array_slice( $tmp, 1 ) ) . ')';
    }
    $command .= $this->buildFlags( [ static::FLAG_FORUPDATE, static::FLAG_LOCKINSHAREMODE ] );

    // adding union custom
    $customs = $this->customs;
    if( isset( $customs[ static::CUSTOM_UNION ] ) ) {
      foreach( $customs[ static::CUSTOM_UNION ] as $union_data ) {
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
    $flags = [ static::FLAG_LOWPRIORITY, static::FLAG_HIGHPRIORITY, static::FLAG_IGNORE ];
    if( !isset( $this->customs[ static::CUSTOM_SELECT ] ) ) array_splice( $flags, 1, 0, static::FLAG_DELAYED );
    $command .= $this->buildFlags( $flags );

    // adding the first table
    foreach( $this->tables as $definition ) if( !is_array( $definition ) ) {
      $command .= ' ' . $this->buildExpression( $definition );
      break;
    }

    // add partitions from custom
    if( !empty( $this->customs[ static::CUSTOM_PARTITION ] ) ) {
      $command .= ' PARTITION (' . implode( ',', $this->customs[ static::CUSTOM_PARTITION ] ) . ')';
    }

    // add table fields
    $tmp = [ ];
    foreach( $this->fields as $alias => $definition ) $tmp[] = ' ' . $this->buildExpression( $alias );
    $command .= '(' . implode( ',', $tmp ) . ' )';

    // add custom select command if any (and ignore simple and batch insertion)
    if( isset( $this->customs[ static::CUSTOM_SELECT ] ) ) {

      $select = $this->customs[ static::CUSTOM_SELECT ][ 0 ];
      $command .= ' ' . $select;

      // adding fields for batch (mass) insertion
    } else if( $this->exist( 'batch:' ) ) {

      $command .= ' VALUES{field_batch}';
      $this->set( 'field_batch', $this->getArray( 'batch:' ) );

      // adding fields for simple insertion
    } else {

      $value_array = [ ];
      foreach( $this->fields as $alias => $definition ) {
        $value_array[] = $this->buildExpression( $alias == $definition ? "{field_{$alias}}" : $definition );
      }
      $command .= ' VALUES(' . implode( ',', $value_array ) . ' )';

      // add fields to the insertion
      $this->each( function ( $key, $value, $index, $builder ) {
        /** @var Builder $builder */
        $builder->set( 'field_' . $key, $value );
      }, 'field:' );
    }

    // add duplicate conditions
    if( isset( $this->customs[ static::CUSTOM_ONDUPLICATE ] ) ) {
      $command .= ' ON DUPLICATE KEY UPDATE ' . implode( ',', $this->customs[ static::CUSTOM_ONDUPLICATE ] );
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
    $command .= $this->buildFlags( [ static::FLAG_LOWPRIORITY, static::FLAG_IGNORE ] );

    // adding tables
    $tmp = [ ];
    foreach( $this->tables as $alias => $definition ) if( !is_array( $definition ) ) {
      $tmp[] = $this->buildExpression( $definition, $alias );
    }
    $command .= implode( ',', $tmp );

    // adding joins
    foreach( $this->tables as $alias => $data ) if( is_array( $data ) ) {
      $command .= ' ' . strtoupper( $data[ 'type' ] ) . ' JOIN' . $this->buildExpression( $data[ 'definition' ], $alias ) . ' ON ' . $data[ 'condition' ];
    }

    // adding fields
    $fields      = $this->fields;
    $field_array = [ ];
    foreach( $fields as $alias => $definition ) {
      $field_array[] = $this->buildOperation( $definition == $alias ? "{field_{$alias}}" : $definition, $alias, '=' );
    }
    $command .= ' SET' . implode( ',', $field_array );

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
    $command .= $this->buildFlags( [ static::FLAG_LOWPRIORITY, static::FLAG_QUICK, static::FLAG_IGNORE ] );

    // adding fields that is tables in this context of Builder
    if( $multi ) {

      foreach( $this->fields as $alias => $name ) $field_array[] = $this->buildExpression( $name, $alias );
      $command .= empty( $field_array ) ? '' : implode( ',', $field_array );
    }

    // preparing tables
    $tmp = [ ];
    foreach( $this->tables as $alias => $name ) if( !is_array( $name ) ) {

      // alias not available in single table
      $tmp[] = $this->buildExpression( $name, $multi ? $alias : $name );
    }

    // add tables
    $command .= ' FROM' . implode( ',', $tmp );

    // adding joins in multitable syntax
    if( $multi ) {

      foreach( $this->tables as $alias => $data ) if( is_array( $data ) ) {
        $command .= ' ' . strtoupper( $data[ 'type' ] ) . ' JOIN' . $this->buildExpression( $data[ 'definition' ], $alias ) . ' ON ' . $data[ 'condition' ];
      }
    }

    // add partitions in single table syntax
    if( !$multi && !empty( $this->customs[ static::CUSTOM_PARTITION ] ) ) {
      $command .= ' PARTITION (' . implode( ',', $this->customs[ static::CUSTOM_PARTITION ] ) . ')';
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
   * Build expression with alias. This will convert Builder class to valid expression or quote it if needed
   *
   * @since 0.10.0
   *
   * @param string|Builder $expression
   * @param string         $alias
   *
   * @return string
   */
  protected function buildExpression( $expression, $alias = null ) {

    // define the alias part if needed
    $alias = empty( $alias ) || $alias == $expression ? '' : ( ' AS ' . $this->dbq->quoteName( $alias ) );

    // define the expression
    if( $expression instanceof Builder ) $expression = "({$expression})";
    else if( preg_match( static::PATTERN_IDENTIFIER, $expression ) ) $expression = $this->dbq->quoteName( $expression );

    return ' ' . $expression . $alias;
  }
  /**
   * Build " {field} {operator} {expression}" like strings
   *
   * @since 1.1.2
   *
   * @param string|Builder $expression
   * @param string|null    $field
   * @param string|null    $operator
   *
   * @return string
   */
  protected function buildOperation( $expression, $field, $operator ) {
    return ' ' . $this->dbq->quoteName( $field ) . ' ' . $operator . $this->buildExpression( $expression );
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
      $result[] = $this->buildExpression( $data[ 0 ] ) . ( isset( $data[ 1 ] ) ? ( ' ' . strtoupper( $data[ 1 ] ) ) : '' );
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
