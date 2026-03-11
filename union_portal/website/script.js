let slides = document.querySelectorAll(".slides img")

let index = 0

setInterval(()=>{

slides[index].style.display="none"

index++

if(index==slides.length){
index=0
}

slides[index].style.display="block"

},3000)