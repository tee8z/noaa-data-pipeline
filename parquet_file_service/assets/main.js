function hello(){
    console.log('hello');
}

var helloButton = document.getElementById("helloButton");

// Add a click event listener to the button
helloButton.addEventListener('click', hello);