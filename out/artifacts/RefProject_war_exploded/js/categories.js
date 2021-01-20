/**
 * view-controller for categories.html
 *
 * M151: RefProject
 *
 * @author  Marcel Suter
 * @since   2020-10-19
 * @version 1.0
 */

/**
 * loads all the books
 */
$(document).ready(function () {
    loadCategories();
});

/**
 * loads the categories from the webservice
 *
 */
function loadCategories() {
    $
        .ajax({
            url: "./resource/category/list",
            dataType: "json",
            type: "GET"
        })
        .done(showCategories)
        .fail(function (xhr) {
            if (xhr.status == 403) {
                location.href = "./login.html";
            } else {
                showMessage("error", "Fehler beim Lesen der Kategorien");
            }
        })
}

/**
 * shows all categories as a table
 *
 * @param categoryData all categories as an array
 */
function showCategories(categoryData) {
    var tableData = "";
    $.each(categoryData, function (index, category) {
        tableData += "<tr>";
        tableData += "<td>" + category.categoryUUID + "</td>";
        tableData += "<td>" + category.title + "</td>";
        tableData += "</tr>";
    });
    $("#categories > tbody").html(tableData);
    showMessage("empty", " ");
}

/**
 * shows a bootstrap alert-message
 * @param type
 * @param message
 */
function showMessage(type, message) {
    var classes = "";
    if (type !== "empty")
        classes="alert alert-" + type;

    $("#message")
        .removeClass()
        .addClass(classes)
        .text(message);
}