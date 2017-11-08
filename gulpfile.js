var gulp = require("gulp");
var ts = require("gulp-typescript");

gulp.task('copy-resource', function () {
    return gulp.src(['./src/**/*.json', './src/**/*.xlsx'])
        .pipe(gulp.dest("dist"));
});
gulp.task('compact-js', function () {
    var tsProject = ts.createProject('tsconfig.json');
    return tsProject.src() // instead of gulp.src(...)
        .pipe(tsProject()).js
        .pipe(gulp.dest('dist'))
});
gulp.task('default', ['copy-resource']);
gulp.task('build', ['compact-js', 'copy-resource']);