<?php namespace Spoom\Sql\MySQL;

use Spoom\Sql;

/**
 * Class Result
 *
 * @property-read \mysqli_result|bool $result
 */
class Result extends Sql\Result {

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
   * Convert field values based on their types
   *
   * @var bool
   */
  private $_convert = true;

  //
  public function __construct( string $statement, $result = null, ?\Throwable $exception = null, int $rows = 0, ?int $insert_id = null ) {
    parent::__construct( $statement, $result, $exception, $rows, $insert_id );

    $this->data = is_object( $result );
    $this->meta = $this->data && $result ? $this->getResult()->fetch_fields() : null;
  }

  /**
   * Free the stored result
   */
  public function free() {
    if( $this->data ) {
      @$this->getResult()->free();
    }

    parent::free();
  }

  //
  public function get( int $record = 0, int $field = 0 ) {
    if( !$this->data || $this->getException() ) return null;

    if( is_string( $field ) ) $row = $this->getAssoc( $record );
    else $row = $this->getArray( $record );

    return isset( $row[ $field ] ) ? $row[ $field ] : null;
  }
  //
  public function getList( int $field = 0 ): array {
    $tmp     = $this->getArrayList();
    $returns = [];

    foreach( $tmp as $v ) {
      $returns[] = $v[ $field ];
    }

    return $returns;
  }

  //
  public function getAssoc( int $record = 0 ): array {
    if( !$this->data || $this->getException() ) return null;

    $result = $this->getResult();
    return @$result->data_seek( $record ) ? $this->process( @$result->fetch_assoc() ) : null;
  }
  //
  public function getAssocList( $index = null ): array {
    if( !$this->data || $this->getException() ) return [];

    $result = $this->getResult();
    if( @!$result->data_seek( 0 ) ) return [];
    else {

      $return_array = [];
      if( $index === null ) while( $row = @$result->fetch_assoc() ) $return_array[] = $this->process( $row );
      else while( $row = @$result->fetch_assoc() ) {

        $row = $this->process( $row );
        if( !isset( $row[ $index ] ) ) $return_array[] = $row;
        else {

          $i = $row[ $index ];
          if( !isset( $return_array[ $i ] ) ) $return_array[ $i ] = $row;
          else {

            if( !is_array( $return_array[ $i ] ) ) $return_array[ $i ] = [ $return_array[ $i ] ];
            $return_array[ $i ][] = $row;

          }
        }
      }

      return $return_array;
    }
  }

  //
  public function getObject( int $record = 0 ) {
    if( !$this->data || $this->getException() ) return null;

    $result = $this->getResult();
    return @$result->data_seek( $record ) ? $this->process( @$result->fetch_object() ) : null;
  }
  //
  public function getObjectList( $index = null ): array {
    if( !$this->data || $this->getException() ) return [];

    $result = $this->getResult();
    if( @$result->data_seek( 0 ) ) return [];
    else {

      $return_array = [];
      if( $index === null ) while( $row = @$result->fetch_object() ) $return_array[] = $this->process( $row );
      else while( $row = @$result->fetch_object() ) {

        $row = $this->process( $row );
        if( !isset( $row->{$index} ) ) $return_array[] = $row;
        else {

          $i = $row->{$index};
          if( !isset( $return_array[ $i ] ) ) $return_array[ $i ] = $row;
          else {

            if( !is_array( $return_array[ $i ] ) ) $return_array[ $i ] = [ $return_array[ $i ] ];
            $return_array[ $i ][] = $row;

          }
        }
      }

      return $return_array;
    }
  }

  //
  public function getArray( int $record = 0 ): array {
    if( !$this->data || $this->getException() ) return [];
    else {

      $result = $this->getResult();
      return @$result->data_seek( $record ) ? $this->process( @$result->fetch_row(), true ) : [];
    }
  }
  //
  public function getArrayList( $index = null ): array {
    if( !$this->data || $this->getException() ) return [];
    else {

      $result = $this->getResult();
      @$result->data_seek( 0 );

      $return_array = [];
      if( $index === null ) while( $row = @$result->fetch_row() ) $return_array[] = $this->process( $row, true );
      else while( $row = @$result->fetch_row() ) {

        $row = $this->process( $row, true );
        if( !isset( $row[ $index ] ) ) $return_array[] = $row;
        else {

          $i = $row[ $index ];
          if( !isset( $return_array[ $i ] ) ) $return_array[ $i ] = $row;
          else {

            if( !is_array( $return_array[ $i ] ) ) $return_array[ $i ] = [ $return_array[ $i ] ];
            $return_array[ $i ][] = $row;

          }
        }
      }

      return $return_array;
    }
  }

  /**
   * Post process rows to convert types to the proper format
   *
   * @param object|array $row     The row to process
   * @param bool         $indexed The array is numeric indexed
   *
   * @return array|object
   */
  private function process( $row, bool $indexed = false ) {

    // process only when meta is available
    if( $this->isConvert() && !empty( $this->meta ) ) {

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

            $data = $data !== null ? (float) $data : null;
            break;

          // integer types
          case MYSQLI_TYPE_INT24:
          case MYSQLI_TYPE_LONG:
          case MYSQLI_TYPE_LONGLONG:
          case MYSQLI_TYPE_SHORT:
          case MYSQLI_TYPE_TINY:

            if( is_null( $data ) ) $data = null;
            else if( $data < PHP_INT_MAX && $data > PHP_INT_MIN ) {
              $data = (int) $data;
            }

            break;
        }

        if( $indexed ) $row[ $index ] = $data;
        else $row[ $meta->name ] = $data;
      }
    }

    return empty( $is_object ) ? $row : (object) $row;
  }

  /**
   * @since 1.2.1
   *
   * @return \mysqli_result|bool
   */
  public function getResult() {
    return parent::getResult();
  }

  /**
   * @return bool
   */
  public function isConvert(): bool {
    return $this->_convert;
  }
  /**
   * @param bool $value
   */
  public function setConvert( bool $value ) {
    $this->_convert = $value;
  }
}
