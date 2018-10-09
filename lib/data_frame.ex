defmodule DataFrame do
  @moduledoc """
    Functions to create and modify a Frame, a structure with a 2D table with information, indexes and columns
  """
  alias DataFrame.Table
  alias DataFrame.Frame

  @doc """
    Creates a new Frame from a 2D table, It creates a numeric index and a numeric column array automatically.
  """
  def new(values) do
    index = autoindex_for_values_dimension(values, 0)
    columns = autoindex_for_values_dimension(values, 1)
    new(values, columns, index)
  end

  @doc """
    Creates a new Frame from a 2D table, and a column array. It creates a numeric index automatically.
  """
  def new(values, columns) when is_list(columns) do
    index = autoindex_for_values_dimension(values, 0)
    new(values, columns, index)
  end

  @doc """
    Creates a new Frame from a 2D table, an index and a column array
  """
  @spec new(Table.t() | list, list, list) :: Frame.t()
  def new(table, columns, index) when is_list(index) and is_list(columns) do
    values = Table.new(table)
    %Frame{values: values, index: index, columns: columns}
  end

  defp autoindex_for_values_dimension(values, dimension) do
    table_dimension = values |> Table.new() |> Table.dimensions() |> Enum.at(dimension)

    if table_dimension == 0 do
      []
    else
      Enum.to_list(0..(table_dimension - 1))
    end
  end

  @doc """
    Creates a Frame from the textual output of a frame (allows copying data from webpages, etc.)
  """
  @spec parse(String.t()) :: Frame.t()
  def parse(text) do
    [header | data] = String.split(text, "\n", trim: true)
    columns = String.split(header, " ", trim: true)
    data_values = data |> Table.new() |> Table.map_rows(&String.split(&1, " ", trim: true))
    [values, index] = Table.remove_column(data_values, 0, return_column: true)
    values_data = Table.map(values, &infer_type/1)
    columns_data = Enum.map(columns, &infer_type/1)
    index_data = Enum.map(index, &infer_type/1)
    new(values_data, columns_data, index_data)
  end

  @doc ~S"""
  Convert element to infered type.
  In case of map, lists and frames converts their values.

  ## Examples

    iex> DataFrame.infer_type("10")
    10

    iex> DataFrame.infer_type("10.1")
    10.1

    iex> DataFrame.infer_type("false")
    false

    iex> DataFrame.infer_type("2018-01-01")
    ~D[2018-01-01]

    iex> DataFrame.infer_type("lflkj123f")
    "lflkj123f"

    iex> DataFrame.infer_type(["1","2",3])
    [1,2,3]

    iex> DataFrame.infer_type(%{a: "10", b: 10})
    %{a: 10, b: 10}

    iex> DataFrame.infer_type(DataFrame.new([["1","2"]], [:a, :b]))
    DataFrame.new([[1, 2]], [:a, :b])
  """
  @spec infer_type(any) :: number | boolean | String.t() | Date.t()

  def infer_type(%Frame{} = frame) do
    map(frame, &infer_type/1, annotated: false)
  end

  def infer_type(elements) when is_map(elements) do
    Enum.into(elements, %{}, fn {k, v} -> {k, infer_type(v)} end)
  end

  def infer_type(elements) when is_list(elements) do
    Enum.map(elements, &infer_type/1)
  end

  def infer_type(element) do
    types = [&try_int/1, &try_float/1, &try_boolean/1, &try_date/1, &try_string/1]

    Enum.reduce_while(types, element, fn fun, value ->
      case fun.(value) do
        :error -> {:cont, value}
        converted -> {:halt, converted}
      end
    end)
  end

  @doc ~S"""
  Force convert element to certain type.
  In case of map and lists converts their values.

  ## Examples

    iex> DataFrame.as_type!("10", :int)
    10

    iex> DataFrame.as_type!("10.1", :float)
    10.1

    iex> DataFrame.as_type!("false", :boolean)
    false

    iex> DataFrame.as_type!("2018-01-01", :date)
    ~D[2018-01-01]

    iex> DataFrame.as_type!(["1","2",3], :int)
    [1,2,3]

    iex> DataFrame.as_type!(%{a: "10", b: 10}, :int)
    %{a: 10, b: 10}
  """
  @type types :: :int | :float | :boolean | :date | :string
  @spec as_type!(any, types) :: number | boolean | String.t() | Date.t()
  def as_type!(elements, type) when is_list(elements) do
    Enum.map(elements, &as_type!(&1, type))
  end

  def as_type!(elements, type) when is_map(elements) do
    Enum.into(elements, %{}, fn {k, v} -> {k, as_type!(v, type)} end)
  end

  def as_type!(element, type) do
    converter =
      case type do
        :int -> &try_int/1
        :float -> &try_float/1
        :boolean -> &try_boolean/1
        :date -> &try_date/1
        :string -> &try_string/1
      end

    case converter.(element) do
      :error -> raise "failed to convert #{inspect(element)} to #{inspect(type)}"
      value -> value
    end
  end

  defp try_int(element) when is_integer(element), do: element
  defp try_int(element) when is_number(element), do: round(element)

  defp try_int(element) when is_binary(element) do
    case Integer.parse(element) do
      {value, ""} -> value
      _ -> :error
    end
  end

  defp try_int(_), do: :error

  defp try_float(element) when is_number(element), do: element

  defp try_float(element) when is_binary(element) do
    case Float.parse(element) do
      {value, ""} -> value
      _ -> :error
    end
  end

  defp try_float(_), do: :error

  defp try_boolean(element) when is_boolean(element), do: element
  defp try_boolean(element) when is_number(element), do: round(element) != 0

  defp try_boolean(element) when is_binary(element) do
    case String.trim(element) do
      "true" -> true
      "false" -> false
      _ -> :error
    end
  end

  defp try_boolean(_), do: :error

  defp try_date(%Date{} = element), do: element

  defp try_date(element) do
    case Date.from_iso8601(element) do
      {:ok, value} -> value
      {:error, _} -> :error
    end
  end

  defp try_string(element) when is_binary(element), do: element

  defp try_string(element) do
    to_string(element)
  end

  # ##################################################
  #  Transforming and Sorting
  # ##################################################

  @doc """
    Returns a Frame which data has been transposed.
  """
  @spec transpose(Frame.t()) :: Frame.t()
  def transpose(frame) do
    %Frame{values: Table.transpose(frame.values), index: frame.columns, columns: frame.index}
  end

  @doc """
  Creates a list of Dataframes grouped by one of the columns.
  A , B
  1 , 2
  1,  3
  2, 4
  group_by(A)
  [ A B
    1 2
    1 3,
    A B
    2 4
  ]
  """
  def group_by(frame, master_column) do
    frame
    |> column(master_column)
    |> Enum.uniq()
    |> Enum.map(fn value -> filter_rows(frame, master_column, value) end)
  end

  @doc ~S"""
  Transforms dataframe to list of maps.

  ## Examples

    iex> DataFrame.to_list_of_maps(DataFrame.new([[1,2],[3,4]], ["A", "B"]))
    [%{"A" => 1, "B" => 2}, %{"A" => 3, "B" => 4}]
  """
  @spec to_list_of_maps(Frame.t()) :: [map]
  def to_list_of_maps(frame) do
    Enum.map(frame.values, fn values ->
      Enum.into(Enum.zip(frame.columns, values), %{})
    end)
  end

  @doc ~S"""
  Rename columns with mapper.

  ## Examples

    iex> DataFrame.rename(DataFrame.new([[1,2],[3,4]], ["A", "B"]), %{"A" => :a})
    DataFrame.new([[1,2],[3,4]], [:a, "B"])
  """
  @spec rename(Frame.t(), map) :: Frame.t()
  def rename(frame, column_names) do
    new_columns =
      Enum.map(frame.columns, fn column ->
        Map.get(column_names, column, column)
      end)

    new(frame.values, new_columns)
  end

  @doc ~S"""
  Returns a frame where each row is the result of invoking fun on each corresponding row.

  Rows are annotated by default, you can switch it off by using `annotated: false` option.

  ## Examples

    iex> DataFrame.map(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> %{ row | "A" => row["A"] * 10 } end)
    DataFrame.new([[10,2],[30,4]], ["A", "B"])

    iex> DataFrame.map(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> List.update_at(row, 0, &(&1 * 10)) end, annotated: false)
    DataFrame.new([[10,2],[30,4]], ["A", "B"])
  """
  @spec map(Frame.t(), (map -> map)) :: Frame.t()
  def map(frame, fun, opts \\ []) do
    annotated? = Keyword.get(opts, :annotated, true)

    values =
      Enum.map(frame.values, fn values ->
        row =
          if annotated? do
            Enum.into(Enum.zip(frame.columns, values), %{})
          else
            values
          end

        new_row = fun.(row)

        if annotated? do
          Enum.map(frame.columns, &Map.fetch!(new_row, &1))
        else
          new_row
        end
      end)

    new(values, frame.columns)
  end

  @doc ~S"""
  Maps the given fun over enumerable and flattens the result.

  Rows are annotated by default, you can switch it off by using `annotated: false` option.

  ## Examples
    iex> DataFrame.flat_map(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> [%{row | "A" => row["A"] * 10}, %{row | "A" => row["A"] * 20}] end)
    DataFrame.new([[10,2], [20,2], [30,4], [60,4]], ["A", "B"])

    iex> DataFrame.flat_map(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> [List.update_at(row, 0, &(&1 * 10)), List.update_at(row, 0, &(&1 * 20))] end, annotated: false)
    DataFrame.new([[10,2], [20,2], [30,4], [60,4]], ["A", "B"])
  """
  @spec flat_map(Frame.t(), (map -> map)) :: Frame.t()
  def flat_map(frame, fun, opts \\ []) do
    annotated? = Keyword.get(opts, :annotated, true)

    values =
      Enum.flat_map(frame.values, fn values ->
        row =
          if annotated? do
            Enum.into(Enum.zip(frame.columns, values), %{})
          else
            values
          end

        new_rows = fun.(row)

        if annotated? do
          Enum.map(new_rows, fn new_row ->
            Enum.map(frame.columns, &Map.fetch!(new_row, &1))
          end)
        else
          new_rows
        end
      end)

    new(values, frame.columns)
  end

  @doc ~S"""
  Filters the frame rows, i.e. returns only those rows for which fun returns a truthy value.

  Rows are annotated by default, you can switch it off by using `annotated: false` option.

  ## Examples

    iex> DataFrame.filter(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> row["A"] > 2 end)
    DataFrame.new([[3,4]], ["A", "B"])

    iex> DataFrame.filter(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> hd(row) > 2 end, annotated: false)
    DataFrame.new([[3,4]], ["A", "B"])
  """
  def filter(frame, fun, opts \\ []) do
    annotated? = Keyword.get(opts, :annotated, true)

    values =
      Enum.filter(frame.values, fn values ->
        row =
          if annotated? do
            Enum.into(Enum.zip(frame.columns, values), %{})
          else
            values
          end

        fun.(row)
      end)

    new(values, frame.columns)
  end

  @doc ~S"""
  Returns a list of rows in table excluding those for which the function fun returns a truthy value.

  Rows are annotated by default, you can switch it off by using `annotated: false` option.

  ## Examples

    iex> DataFrame.reject(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> row["A"] > 2 end)
    DataFrame.new([[1,2]], ["A", "B"])

    iex> DataFrame.reject(DataFrame.new([[1,2],[3,4]], ["A", "B"]), fn row -> hd(row) > 2 end, annotated: false)
    DataFrame.new([[1,2]], ["A", "B"])
  """
  def reject(frame, fun, opts \\ []) do
    annotated? = Keyword.get(opts, :annotated, true)

    values =
      Enum.reject(frame.values, fn values ->
        row =
          if annotated? do
            Enum.into(Enum.zip(frame.columns, values), %{})
          else
            values
          end

        fun.(row)
      end)

    new(values, frame.columns)
  end

  @doc """
    Sorts the data in the frame based on its index. By default the data is sorted in ascending order.
  """
  @spec sort_index(Frame.t(), boolean) :: Frame.t()
  def sort_index(frame, ascending \\ true) do
    sort(frame, 0, ascending)
  end

  @doc """
    Sorts the data in the frame based on a given column. By default the data is sorted in ascending order.
  """
  @spec sort_values(Frame.t(), String.t(), boolean) :: Frame.t()
  def sort_values(frame, column_name, ascending \\ true) do
    index = Enum.find_index(frame.columns, fn x -> x == column_name end)
    sort(frame, index + 1, ascending)
  end

  defp sort(frame, column_index, ascending) do
    sorting_func =
      if ascending do
        fn x, y -> Enum.at(x, column_index) > Enum.at(y, column_index) end
      else
        fn x, y -> Enum.at(x, column_index) < Enum.at(y, column_index) end
      end

    [values, index] =
      frame.values
      |> Table.append_column(frame.index)
      |> Table.sort_rows(fn x, y -> sorting_func.(x, y) end)
      |> Table.remove_column(0, return_column: true)

    DataFrame.new(values, frame.columns, index)
  end

  # ##################################################
  #  Selecting
  # ##################################################

  @doc """
  Returns the information at the top of the frame. Defaults to 5 lines.
  """
  @spec head(Frame.t(), integer) :: Frame.t()
  def head(frame, size \\ 5) do
    DataFrame.new(Enum.take(frame.values, size), frame.columns, Enum.take(frame.index, size))
  end

  @doc """
  Returns the information at the bottom of the frame. Defaults to 5 lines.
  """
  @spec tail(Frame.t(), integer) :: Frame.t()
  def tail(frame, the_size \\ 5) do
    size = -the_size
    head(frame, size)
  end

  @doc """
  Generic method to return rows based on the value of the index
  """
  def rows(frame, first..last) when is_integer(first) and is_integer(last) do
    irows(frame, indexes_by_named_range(frame.index, first..last))
  end

  def rows(frame, row_names) when is_list(row_names) do
    irows(frame, indexes_by_name(frame.index, row_names))
  end

  @doc """
  Generic method to return rows based on the position of the index
  """
  def irows(frame, first..last) when is_integer(first) and is_integer(last) do
    irows(frame, Enum.to_list(first..last))
  end

  def irows(frame, row_indexes) when is_list(row_indexes) do
    rows = multiple_at(frame.index, row_indexes)
    values = Table.rows(frame.values, row_indexes)
    DataFrame.new(values, frame.columns, rows)
  end

  @doc """
  Returns a Frame with the selected columns by name.
  """
  def columns(frame, first..last) when is_integer(first) and is_integer(last) do
    icolumns(frame, indexes_by_named_range(frame.columns, first..last))
  end

  def columns(frame, column_names) when is_list(column_names) do
    icolumns(frame, indexes_by_name(frame.columns, column_names))
  end

  @doc ~S"""
  Appends derived column to table.

  ## Examples

    iex> DataFrame.append_column(DataFrame.new([[1],[2]], [:a]), :b, fn _ -> 10 end)
    DataFrame.new([[1,10], [2, 10]], [:a, :b])
  """
  @spec append_column(Frame.t(), any, (any -> any)) :: Frame.t()
  def append_column(frame, column_name, fun, opts \\ []) do
    append_columns(frame, [column_name], fn row -> [fun.(row)] end, opts)
  end

  @doc ~S"""
  Appends derived columns to table.

  ## Examples

    iex> DataFrame.append_columns(DataFrame.new([[1],[2]], [:a]), [:b, :c], fn _ -> [10, 20] end)
    DataFrame.new([[1,10, 20], [2, 10, 20]], [:a, :b, :c])
  """
  @spec append_columns(Frame.t(), any, (any -> [any])) :: Frame.t()
  def append_columns(frame, column_names, fun, opts \\ []) do
    annotated? = Keyword.get(opts, :annotated, true)

    values =
      Enum.map(frame.values, fn values ->
        row =
          if annotated? do
            Enum.into(Enum.zip(frame.columns, values), %{})
          else
            values
          end

        new_values = fun.(row)
        values ++ new_values
      end)

    columns = frame.columns ++ column_names

    new(values, columns)
  end

  @doc """
  Returns a Frame with the selected columns by position.
  """
  def icolumns(frame, first..last) when is_integer(first) and is_integer(last) do
    icolumns(frame, Enum.to_list(first..last))
  end

  def icolumns(frame, column_indexes) when is_list(column_indexes) do
    columns = multiple_at(frame.columns, column_indexes)
    values = Table.columns(frame.values, column_indexes)
    DataFrame.new(values, columns, frame.index)
  end

  @doc """
    Returns the data in the frame.
    Parameters are any list of rows and columns with names or a ranges of names
    To get only rows or columns check the functions above
  """
  @spec loc(Frame.t(), Range.t() | list(), Range.t() | list()) :: Frame.t()
  def loc(frame, row_names, column_names) do
    frame |> rows(row_names) |> columns(column_names)
  end

  @doc """
    Returns a slice of the data in the frame.
    Parameters are any list of rows and columns
  """
  @spec iloc(Frame.t(), Range.t() | list(integer), Range.t() | list(integer)) :: Frame.t()
  def iloc(frame, row_index, column_index) do
    frame |> irows(row_index) |> icolumns(column_index)
  end

  # TODO: move somewhere
  # same than .at but accepting a list of indexes
  defp multiple_at(list, list_index) do
    list_index
    |> Enum.map(fn index -> Enum.at(list, index) end)
    |> Enum.filter(fn element -> element != nil end)
  end

  defp indexes_by_named_range(list, first..last) do
    first_index = Enum.find_index(list, fn x -> to_string(x) == to_string(Enum.at(first, 0)) end)
    last_index = Enum.find_index(list, fn x -> to_string(x) == to_string(Enum.at(last, 0)) end)
    Enum.to_list(first_index..last_index)
  end

  defp indexes_by_name(name_list, selected_names) when is_list(selected_names) do
    indexes =
      name_list
      |> Enum.with_index()
      |> Enum.reduce([], fn tuple, acc ->
        if Enum.member?(selected_names, elem(tuple, 0)) do
          [elem(tuple, 1) | acc]
        else
          acc
        end
      end)

    Enum.reverse(indexes)
  end

  @doc """
    Returns a value located at the position indicated by an index name and column name.
  """
  @spec at(Frame.t(), String.t(), String.t()) :: any()
  def at(frame, index_name, column_name) do
    index = Enum.find_index(frame.index, fn x -> to_string(x) == to_string(index_name) end)
    column = Enum.find_index(frame.columns, fn x -> to_string(x) == to_string(column_name) end)
    DataFrame.iat(frame, column, index)
  end

  @doc """
    Returns a value located at the position indicated by an index position and column position.
  """
  @spec iat(Frame.t(), integer, integer) :: any()
  def iat(frame, index, column) do
    Table.at(frame.values, index, column)
  end

  @doc """
  Returns a list of data, not a frame like object. with the values of a given column
  """
  @spec column(Frame.t(), String.t()) :: list()
  def column(frame, column_name) do
    column = Enum.find_index(frame.columns, fn x -> to_string(x) == to_string(column_name) end)
    frame.values |> Table.columns([column]) |> Table.to_row_list() |> List.flatten()
  end

  @doc """
    Experimental
    Returns the rows that contains certain value in a column
    # TODO: rationalize all this slicing operations
  """
  def filter_rows(frame, expected_column_name, expected_value) do
    column_index = Enum.find_index(frame.columns, fn x -> x == expected_column_name end)

    if column_index == nil do
      frame
    else
      values =
        Table.map_rows(
          frame.values,
          fn row ->
            if Enum.at(row, column_index) == expected_value do
              row
            else
              [nil]
            end
          end
        )

      {new_values, new_index} = delete_nil_rows(values, frame.index)
      DataFrame.new(new_values, frame.columns, new_index)
    end
  end

  defp delete_nil_rows([], _) do
    {Table.new(), []}
  end

  defp delete_nil_rows(table, list) do
    nil_index =
      Enum.find_index(table, fn row -> Enum.all?(row, fn element -> element == nil end) end)

    if nil_index == nil do
      {table, list}
    else
      delete_nil_rows(List.delete_at(table, nil_index), List.delete_at(list, nil_index))
    end
  end

  # ##################################################
  #  Mathematics
  # ##################################################

  @doc """
    Returns the cummulative sum
  """
  @spec cumsum(Frame.t()) :: Frame.t()
  def cumsum(frame) do
    cumsummed =
      frame.values
      |> Table.map_columns(fn column ->
        Enum.flat_map_reduce(column, 0, fn x, acc ->
          {[x + acc], acc + x}
        end)
      end)

    data = Enum.at(cumsummed, 0)
    DataFrame.new(Table.transpose(data), frame.columns)
  end

  @doc """
    Returns a statistical description of the data in the frame
  """
  @spec describe(Frame.t()) :: Frame.t()
  def describe(frame) do
    DataFrame.Statistics.describe(frame)
  end

  # ##################################################
  #  Importing, exporting, plotting
  # ##################################################

  @doc """
    Writes the information of the frame into a csv file. By default the column names are written also
  """
  def to_csv(frame, filename, header \\ true) do
    file = File.open!(filename, [:write])

    values =
      if header do
        [frame.columns | frame.values]
      else
        frame.values
      end

    values |> CSV.encode() |> Enum.each(&IO.write(file, &1))
  end

  @doc """
    Reads the information from a CSV file. By default the first row is assumed to be the column names.
  """
  @spec from_csv(String.t()) :: Frame.t()
  def from_csv(filename) do
    [headers | values] = filename |> File.stream!() |> CSV.decode!() |> Enum.to_list()
    new(values, headers)
  end

  @doc """
    Reads the information from a CSV string. By default the first row is assumed to be the column names.
  """
  @spec from_string(String.t()) :: Frame.t()
  def from_string(binary) do
    splitted = Regex.split(~r/\r\n|\n/, String.trim(binary))

    [headers | values] =
      splitted
      |> Stream.map(& &1)
      |> CSV.decode!()
      |> Enum.to_list()

    new(values, headers)
  end

  @spec plot(Frame.t()) :: :ok
  def plot(frame) do
    plotter = Explot.new()
    columns_with_index = frame.values |> Table.transpose() |> Enum.with_index()

    Enum.each(columns_with_index, fn column_with_index ->
      column = elem(column_with_index, 0)
      column_name = Enum.at(frame.columns, elem(column_with_index, 1))
      Explot.add_list(plotter, column, column_name)
    end)

    Explot.x_axis_labels(plotter, frame.index)
    Explot.show(plotter)
  end
end

# DataFrame.new(DataFrame.Table.build_random(6,4), [1,3,4,5], DataFrame.DateRange.new("2016-09-12", 6) )
