<?php namespace Sql\Mysql;

use Sql\Result as SqlResult;

/**
 * Class Result
 * @package Sql\Mysql
 *
 * @property-read \mysqli_result|bool $result
 */
class Result extends SqlResult {

  /**
   * Indicate if the query return some result
   *
   * @var bool
   */
  private $data;

  /**
   * Meta properties for the fields
   *
   * @var array|null
   */
  private $meta;

  /**
   * @inheritdoc
   */
  public function __construct( $command, $result = false, $exception = null, $rows = 0, $insert_id = null ) {
    parent::__construct( $command, $result, $exception, $rows, $insert_id );

    $this->data = is_object( $result );
    $this->meta = $this->data ? $this->result->fetch_fields() : null;
  }

  /**
   * Free the stored result
   */
  public function free() {
    if( $this->data ) $this->result->free();

    parent::free();
  }

  /**
   * @inheritdoc
   */
  public function get( $record = 0, $field = 0 ) {
    if( !$this->data || $this->exception ) return null;

    if( is_string( $field ) ) $row = $this->getAssoc( $record );
    else $row = $this->getArray( $record );

    return isset( $row[ $field ] ) ? $row[ $field ] : null;
  }
  /**
   * @inheritdoc
   */
  public function getList( $field = 0 ) {
    $tmp     = $this->getArrayList();
    $returns = [ ];

    foreach( $tmp as $v ) {
      $returns[ ] = $v[ $field ];
    }

    return $returns;
  }

  /**
   * @inheritdoc
   */
  public function getAssoc( $record = 0 ) {
    if( !$this->data || $this->exception ) return null;

    $result = $this->result;
    @$result->data_seek( $record );

    return $this->process( $result->fetch_assoc() );
  }
  /**
   * @inheritdoc
   */
  public function getAssocList( $index = null ) {
    if( !$this->data || $this->exception ) return [ ];

    $result = $this->result;
    @$result->data_seek( 0 );

    $return_array = [ ];
    if( $index === null ) while( $row = $result->fetch_assoc() ) $return_array[ ] = $this->process( $row );
    else while( $row = $result->fetch_assoc() ) {

      $row = $this->process( $row );
      if( !isset( $row[ $index ] ) ) $return_array[ ] = $row;
      else {

        $i = $row[ $index ];
        if( !isset( $return_array[ $i ] ) ) $return_array[ $i ] = $row;
        else {

          if( !is_array( $return_array[ $i ] ) ) $return_array[ $i ] = [ $return_array[ $i ] ];
          $return_array[ $i ][ ] = $row;

        }
      }
    }

    return $return_array;
  }

  /**
   * @inheritdoc
   */
  public function getObject( $record = 0 ) {
    if( !$this->data || $this->exception ) return null;

    $result = $this->result;
    @$result->data_seek( $record );

    return $this->process( $result->fetch_object() );
  }
  /**
   * @inheritdoc
   */
  public function getObjectList( $index = null ) {
    if( !$this->data || $this->exception ) return [ ];

    $result = $this->result;
    @$result->data_seek( 0 );

    $return_array = [ ];
    if( $index === null ) while( $row = $result->fetch_object() ) $return_array[ ] = $this->process( $row );
    else while( $row = $result->fetch_object() ) {

      $row = $this->process( $row );
      if( !isset( $row->{$index} ) ) $return_array[ ] = $row;
      else {

        $i = $row->{$index};
        if( !isset( $return_array[ $i ] ) ) $return_array[ $i ] = $row;
        else {

          if( !is_array( $return_array[ $i ] ) ) $return_array[ $i ] = [ $return_array[ $i ] ];
          $return_array[ $i ][ ] = $row;

        }
      }
    }

    return $return_array;
  }

  /**
   * @inheritdoc
   */
  public function getArray( $record = 0 ) {
    if( !$this->data || $this->exception ) return [ ];

    $result = $this->result;
    @$result->data_seek( $record );

    return $this->process( $result->fetch_row(), true );
  }
  /**
   * @inheritdoc
   */
  public function getArrayList( $index = null ) {
    if( !$this->data || $this->exception ) return [ ];

    $result = $this->result;
    @$result->data_seek( 0 );

    $return_array = [ ];
    if( $index === null ) while( $row = $result->fetch_row() ) $return_array[ ] = $this->process( $row, true );
    else while( $row = $result->fetch_row() ) {

      $row = $this->process( $row, true );
      if( !isset( $row[ $index ] ) ) $return_array[ ] = $row;
      else {

        $i = $row[ $index ];
        if( !isset( $return_array[ $i ] ) ) $return_array[ $i ] = $row;
        else {

          if( !is_array( $return_array[ $i ] ) ) $return_array[ $i ] = [ $return_array[ $i ] ];
          $return_array[ $i ][ ] = $row;

        }
      }
    }

    return $return_array;
  }

  /**
   * Post process rows to convert types to the proper format
   *
   * @param object|array $row     The row to process
   * @param bool         $indexed The array is numeric indexed
   *
   * @return array|object
   */
  private function process( $row, $indexed = false ) {

    // process only when meta is available
    if( !empty( $this->meta ) ) {

      // preprocess the row
      $is_object = is_object( $row );
      $row       = (array) $row;

      // search trough the meta object and convert the available row indexes
      foreach( $this->meta as $index => $meta ) {

        $data = $indexed ? $row[ $index ] : $row[ $meta->name ];
        switch( $meta->type ) {

          // floating point types
          case MYSQLI_TYPE_DECIMAL:
          case MYSQLI_TYPE_NEWDECIMAL:
          case MYSQLI_TYPE_DOUBLE:
          case MYSQLI_TYPE_FLOAT:

            $data = is_null( $data ) ? null : (float) $data;
            break;

          // integer types
          case MYSQLI_TYPE_INT24:
          case MYSQLI_TYPE_LONG:
          case MYSQLI_TYPE_LONGLONG:
          case MYSQLI_TYPE_SHORT:
          case MYSQLI_TYPE_TINY:

            $data = is_null( $data ) ? null : (int) $data;
            break;
        }

        if( $indexed ) $row[ $index ] = $data;
        else $row[ $meta->name ] = $data;
      }
    }

    return empty( $is_object ) ? $row : (object) $row;
  }
}
