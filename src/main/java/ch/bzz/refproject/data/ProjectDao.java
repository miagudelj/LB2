package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
                "SELECT p.projectUUID, p.title, p.status, p.category p.startDate, p.endDate," +
                        "       c.categoryUUID, c.category" +
                        "  FROM Project AS p JOIN Category AS c USING (categoryUUID)" +
                        " ORDER BY status = A";
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
