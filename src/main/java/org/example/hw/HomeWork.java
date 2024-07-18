package org.example.hw;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class HomeWork {
    /**
     * С помощью JDBC, выполнить следующие пункты:
     * 1. Создать таблицу Person (скопировать код с семниара)
     * 2. Создать таблицу Department (id bigint primary key, name varchar(128) not null)
     * 3. Добавить в таблицу Person поле department_id типа bigint (внешний ключ)
     * 4. Написать метод, который загружает Имя department по Идентификатору person
     * 5. * Написать метод, который загружает Map<String, String>, в которой маппинг person.name -> department.name
     *   Пример: [{"person #1", "department #1"}, {"person #2", "department #3}]
     * 6. ** Написать метод, который загружает Map<String, List<String>>, в которой маппинг department.name -> <person.name>
     *   Пример:
     *   [
     *     {"department #1", ["person #1", "person #2"]},
     *     {"department #2", ["person #3", "person #4"]}
     *   ]
     *
     *  7. *** Создать классы-обертки над таблицами, и в пунктах 4, 5, 6 возвращать объекты.
     */
    public static void main(String[] args) {
        try(Connection connection = DriverManager.getConnection("jdbc:h2:mem:testDB")) {
            createTable(connection);
            createTableD(connection);
            addColumTable(connection);
            insertDataD(connection);
            insertData(connection);
            selectData(connection);
            System.out.println(getPersonDepartmentName(connection, 3));


        } catch (SQLException e) {
            System.err.println("Во время подключения произошла ошибка: " + e.getMessage());
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
        create table person (
          id bigint primary key,
          name varchar(256),
          age integer,
          active boolean
        )
        """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы P произошла ошибка: " + e.getMessage());
            throw e;
        }
    }

    private static void createTableD(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
        create table department (
          id bigint primary key, 
          name varchar(128) not null
        )
        """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы D произошла ошибка: " + e.getMessage());
            throw e;
        }
    }

    private static void insertDataD(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into department(id, name) values\n");
            for (int i = 1; i <= 4; i++) {
                insertQuery.append(String.format("(%s, '%s')", i, "Dep #" + i));

                if (i != 4) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк: " + insertCount);
        }
    }

    private static void insertData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into person(id, name, age, active, department_id) values\n");
            for (int i = 1; i <= 10; i++) {
                int age = ThreadLocalRandom.current().nextInt(20, 60);
                boolean active = ThreadLocalRandom.current().nextBoolean();
                long department_id = ThreadLocalRandom.current().nextLong(1, 5);
                insertQuery.append(String.format("(%s, '%s', %s, %s, %s)", i, "Person #" + i, age, active, department_id));

                if (i != 10) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк: " + insertCount);
        }
    }



    private static void addColumTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
        ALTER TABLE person
        ADD COLUMN department_id bigint
        """);
            statement.execute("""
        ALTER TABLE person
        ADD FOREIGN KEY (department_id) REFERENCES department(id)
        """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
            throw e;
        }
    }

    private static void selectData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
        select id, name, age, active, department_id
        from person
        """);

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                boolean active = resultSet.getBoolean("active");
                long department_id = resultSet.getLong("department_id");
                // persons.add(new Person(id, name, age))
                System.out.println("Найдена строка: [id = " + id + ", name = " + name + ", age = " +
                        age  + "active = " + active + ", department_id = " + department_id + "]");
            }
        }
    }

      /**
   * Пункт 4
   */
  private static String getPersonDepartmentName(Connection connection, int personId) throws SQLException {
      // FIXME: Ваш код тут
      try (PreparedStatement statement =
                   connection.prepareStatement("""
                            SELECT department_id
                            FROM person
                            where id = ?;""")) {
          statement.setInt(1, personId);
          ResultSet resultSet = statement.executeQuery();
          int nameDep = Integer.parseInt(resultSet.getString("department_id"));

          try (PreparedStatement statement1 =
                  connection.prepareStatement("""
                            SELECT name
                            FROM department
                            where id = ?;""")) {
              statement1.setInt(1, nameDep);
              ResultSet resultSet1 = statement.executeQuery();
              return resultSet1.getString("name");
          }
      }
  }
}
