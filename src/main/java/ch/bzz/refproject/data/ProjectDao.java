package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectDao implements Dao<Project, String>{
    /**
     * default constructor
     */
    public ProjectDao() {}

    /**
     * reads all projects in the table "Project"
     * @return list of projects
     */
    @Override
    public List<Project> getAll() {
        ResultSet resultSet;
        List<Project> projectList = new ArrayList<>();
        String sqlQuery =
                "SELECT p.projectUUID, p.title, p.status, p.categoryUUID, p.startDate, p.endDate, c.categoryUUID" +
                " FROM Project AS p JOIN Category AS c USING (categoryUUID)" +
                " where status = 'A'" +
                " ORDER BY title";
        try {
            resultSet = MySqlDB.sqlSelect(sqlQuery);
            while (resultSet.next()) {
                Project project = new Project();
                setValues(resultSet, project);
                projectList.add(project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }
        return projectList;

    }

    /**
     * reads a project from the table "Project" identified by the projectUUID
     * @param projectUUID the primary key
     * @return project object
     */
    @Override
    public Project getEntity(String projectUUID) {
        ResultSet resultSet;
        Project project = new Project();

        String sqlQuery = "SELECT p.projectUUID, p.title, p.status, p.categoryUUID, p.startDate, p.endDate, c.categoryUUID" +
                " FROM Project AS p JOIN Category AS c USING (categoryUUID)" +
                " WHERE projectUUID=?";

        Map<Integer, Object> map = new HashMap<>();
        map.put(1, projectUUID);

        try {
            resultSet = MySqlDB.sqlSelect(sqlQuery, map);
            if (resultSet.next()) {
                setValues(resultSet, project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {
            MySqlDB.sqlClose();
        }
        return project;

    }


    /**
     * daletes a project in the table "Project" identified by the projectUUID
     * @param projectUUID the primary key
     * @return Result code
     */
    @Override
    public Result delete(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "DELETE * FROM Project" +
                        " WHERE projectUUID='" + projectUUID + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows == 1) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }

    /**
     * saves a project in the table "Project"
     * @param project the project object
     * @return Result code
     */
    @Override
    public Result save(Project project) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "REPLACE Project" +
                        " SET projectUUID='" + project.getProjectUUID() + "'," +
                        " title='" + project.getTitle() + "'," +
                        " status='" + project.getStatus() + "'," +
                        " categoryUUID='" + project.getCategory().getCategoryUUID() + "'," +
                        " startDate=" + project.getStartDate() + "," +
                        " endDate='" + project.getEndDate() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows <= 2) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }
    
    /**
     * sets the values of the attributes from the resultset
     *
     * @param resultSet the resultSet with an entity
     * @param project   a project object
     * @throws SQLException
     */
    private void setValues(ResultSet resultSet, Project project) throws SQLException {
        project.setProjectUUID(resultSet.getString("projectUUID"));
        project.setTitle(resultSet.getString("title"));
        project.setStatus(resultSet.getString("status"));
        project.setCategory(new Category());
        project.getCategory().setCategoryUUID(resultSet.getString("categoryUUID"));
        project.setStartDate(resultSet.getString("startDate"));
        project.setEndDate(resultSet.getString("endDate"));
    }
}
